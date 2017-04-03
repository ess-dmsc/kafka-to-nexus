#include "roundtrip.h"
#include "../KafkaW.h"
#include "../helper.h"
#include "../logger.h"
#include "../schemas/ev42/ev42_synth.h"
#include "schemas/f141_epics_nt_generated.h"
#include <array>
#include <future>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <string>
#include <type_traits>
#include <vector>

MainOpt *Roundtrip::opt = nullptr;

namespace BrightnESS {
namespace FileWriter {
namespace Test {

using std::string;
using std::array;
using std::vector;

int64_t produce_command_from_string(uri::URI const &uri,
                                    std::string const &cmd) {
  KafkaW::BrokerOpt opt;
  opt.address = uri.host_port;
  auto p = std::make_shared<KafkaW::Producer>(opt);
  std::promise<int64_t> offset;
  std::function<void(rd_kafka_message_t const *msg)> cb = [&offset](
      rd_kafka_message_t const *msg) { offset.set_value(msg->offset); };
  p->on_delivery_ok = cb;
  KafkaW::Producer::Topic pt(p, uri.topic);
  pt.do_copy();
  pt.produce((uint8_t *)cmd.data(), cmd.size());
  p->poll_while_outq();
  auto fut = offset.get_future();
  auto x = fut.wait_for(std::chrono::milliseconds(2000));
  if (x == std::future_status::ready) {
    return fut.get();
  }
  LOG(0, "Timeout on production of test message");
  return -1;
}

/// Produce a command from a json file
int64_t produce_command_from_file(uri::URI const &uri, std::string file) {
  auto v1 = gulp(file.c_str());
  return produce_command_from_string(uri, std::string(v1.data(), v1.size()));
}

template <typename T, typename = int> struct _has_teamid : std::false_type {
  static void fill(T &fwdinfo, uint64_t teamid) {}
};
template <typename T>
struct _has_teamid<T, decltype((void)T::teamid, 0)> : std::true_type {
  static void fill(T &fwdinfo, uint64_t teamid) { fwdinfo.teamid = teamid; }
};

void roundtrip_simple_01(MainOpt &opt) {
  LOG(5, "Run test:  Test::roundtrip_simple_01");
  using namespace BrightnESS::FileWriter;
  using namespace rapidjson;
  using CLK = std::chrono::steady_clock;
  using MS = std::chrono::milliseconds;
  Master m(opt.master_config);
  opt.master = &m;
  auto fn_cmd = "tests/msg-conf-new-01.json";
  auto of = produce_command_from_file(opt.master_config.command_listener.broker,
                                      fn_cmd);
  opt.master_config.command_listener.start_at_command_offset = of - 1;
  std::thread t1([&m] { ASSERT_NO_THROW(m.run()); });

  // We want the streamers to be ready
  // stream_master.wait_until_connected();
  std::this_thread::sleep_for(MS(1000));

  auto json_data = gulp(fn_cmd);
  Document d;
  d.Parse(json_data.data(), json_data.size());
  std::vector<std::string> test_sourcenames;
  std::vector<std::string> test_topics;
  for (auto &x : d["streams"].GetArray()) {
    test_sourcenames.push_back(x["source"].GetString());
    test_topics.push_back(x["topic"].GetString());
  }

  {
    // Produce sample data using the nt types scheme only
    KafkaW::BrokerOpt opt;
    opt.address = "localhost:9092";
    // auto nowns = []{return
    // std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();};
    for (size_t i3 = 0; i3 < test_sourcenames.size(); ++i3) {
      auto prod = std::make_shared<KafkaW::Producer>(opt);
      KafkaW::Producer::Topic topic(prod, test_topics[i3]);
      topic.do_copy();
      auto &sourcename = test_sourcenames[i3];
      for (int i1 = 0; i1 < 2; ++i1) {
        flatbuffers::FlatBufferBuilder builder(1024);
        /*
        FlatBufs::f141_epics_nt::fwdinfo_t fi;
        fi.mutate_seq(i1);
        fi.mutate_ts_data(nowns() + 1000000 * i1);
        fi.mutate_ts_fwd(fi.ts_data());
        fi.mutate_fwdix(0);
        fi.mutate_teamid(0);
        _has_teamid<FlatBufs::f141_epics_nt::fwdinfo_t>::fill(fi, 0);
        */
        std::vector<double> data;
        data.resize(7);
        for (size_t i2 = 0; i2 < data.size(); ++i2) {
          data[i2] = 10000 * (i3 + 1) + 100 * i1 + i2;
        }
        auto value = builder.CreateVector(data);
        FlatBufs::f141_epics_nt::NTScalarArrayDoubleBuilder b1(builder);
        b1.add_value(value);
        auto pv = b1.Finish().Union();
        auto sn = builder.CreateString(sourcename);
        FlatBufs::f141_epics_nt::EpicsPVBuilder epicspv(builder);
        epicspv.add_name(sn);
        epicspv.add_pv_type(FlatBufs::f141_epics_nt::PV::NTScalarArrayDouble);
        epicspv.add_pv(pv);
        // epicspv.add_fwdinfo(&fi);
        FinishEpicsPVBuffer(builder, epicspv.Finish());
        if (true) {
          topic.produce(builder.GetBufferPointer(), builder.GetSize());
          prod->poll();
        }
      }
      prod->poll_while_outq();
    }
    // fwt->file_flush();
  }

  auto start = CLK::now();
  while (CLK::now() - start < MS(5000)) {
    std::this_thread::sleep_for(MS(200));
  }
  LOG(5, "Stop Master");
  m.stop();
  t1.join();
}

TEST_F(Roundtrip, simple_01) {
  // disabled
  return;
  BrightnESS::FileWriter::Test::roundtrip_simple_01(*opt);
}

void roundtrip_remote_kafka(MainOpt &opt, string fn_cmd) {
  LOG(7, "roundtrip_remote_kafka");
  using namespace BrightnESS::FileWriter;
  using namespace rapidjson;
  using CLK = std::chrono::steady_clock;
  using MS = std::chrono::milliseconds;
  using std::string;

  // One broker for commands and data for this test
  auto &broker_common = opt.master_config.command_listener.broker;

  auto json_data = gulp(fn_cmd);
  Document d;
  d.Parse(json_data.data(), json_data.size());
  if (d.HasParseError()) {
    LOG(3, "ERROR can not parse command");
    return;
  }
  auto &a = d.GetAllocator();
  d.RemoveMember("broker");
  d.AddMember("broker", Value(broker_common.host_port.c_str(), a), a);
  StringBuffer cmd_buf1;
  PrettyWriter<StringBuffer> wr(cmd_buf1);
  d.Accept(wr);

  auto of = produce_command_from_string(broker_common, cmd_buf1.GetString());
  opt.master_config.command_listener.start_at_command_offset = of - 1;
  Master m(opt.master_config);
  opt.master = &m;
  std::thread t1([&m] { ASSERT_NO_THROW(m.run()); });

  // We want the streamers to be ready
  // stream_master.wait_until_connected();
  std::this_thread::sleep_for(MS(1000));

  std::vector<std::string> test_sourcenames;
  std::vector<std::string> test_topics;
  for (auto &x : d["streams"].GetArray()) {
    // TODO  we use currently only one stream!
    // TODO  get the fbid from the command and invoke the correct synth
    test_sourcenames.push_back(x["source"].GetString());
    test_topics.push_back(x["topic"].GetString());
    break;
  }

  {
    // Produce sample data using the nt types scheme only
    KafkaW::BrokerOpt bopt;
    bopt.address = broker_common.host_port;
    auto prod = std::make_shared<KafkaW::Producer>(bopt);
    for (size_t i3 = 0; i3 < test_sourcenames.size(); ++i3) {
      KafkaW::Producer::Topic topic(prod, test_topics[i3]);
      topic.do_copy();
      auto &sourcename = test_sourcenames[i3];
      BrightnESS::FileWriter::Msg msg;
      BrightnESS::FlatBufs::ev42::synth synth(sourcename, 1);

      for (int i1 = 0; i1 < 32; ++i1) {
        auto fb = synth.next(64);
        msg =
            BrightnESS::FileWriter::Msg{(char *)fb.builder->GetBufferPointer(),
                                        (int32_t)fb.builder->GetSize() };
        {
          auto v = binary_to_hex(msg.data, msg.size);
          LOG(7, "msg:\n{:.{}}", v.data(), v.size());
        }
        if (true) {
          topic.produce((uint8_t *)msg.data, msg.size);
          prod->poll();
        }
      }
      prod->poll_while_outq();
    }
    // fwt->file_flush();
  }

  auto start = CLK::now();
  while (CLK::now() - start < MS(4000)) {
    std::this_thread::sleep_for(MS(200));
  }
  LOG(5, "Stop Master");
  m.stop();
  t1.join();
}

} // namespace Test
} // namespace FileWriter
} // namespace BrightnESS

TEST_F(Roundtrip, ev42_remote_kafka) {
  BrightnESS::FileWriter::Test::roundtrip_remote_kafka(
      *Roundtrip::opt, "tests/msg-cmd-new-03.json");
}
