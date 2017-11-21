#include "../HDFFile.h"
#include "../CommandHandler.h"
#include "../HDFFile_impl.h"
#include "../KafkaW.h"
#include "../MainOpt.h"
#include "../helper.h"
#include "../schemas/ev42/ev42_synth.h"
#include "../schemas/f142/f142_synth.h"
#include <array>
#include <chrono>
#include <gtest/gtest.h>
#include <random>
#include <rapidjson/document.h>
#include <string>
#include <unistd.h>
#include <vector>

using std::string;
using std::vector;
using std::array;
using std::chrono::steady_clock;
using std::chrono::milliseconds;
using std::chrono::duration_cast;

template <typename T> hid_t nat_type();
template <> hid_t nat_type<float>() { return H5T_NATIVE_FLOAT; }
template <> hid_t nat_type<double>() { return H5T_NATIVE_DOUBLE; }
template <> hid_t nat_type<int8_t>() { return H5T_NATIVE_INT8; }
template <> hid_t nat_type<int16_t>() { return H5T_NATIVE_INT16; }
template <> hid_t nat_type<int32_t>() { return H5T_NATIVE_INT32; }
template <> hid_t nat_type<int64_t>() { return H5T_NATIVE_INT64; }
template <> hid_t nat_type<uint8_t>() { return H5T_NATIVE_UINT8; }
template <> hid_t nat_type<uint16_t>() { return H5T_NATIVE_UINT16; }
template <> hid_t nat_type<uint32_t>() { return H5T_NATIVE_UINT32; }
template <> hid_t nat_type<uint64_t>() { return H5T_NATIVE_UINT64; }

// Verify
TEST(HDFFile, create) {
  auto fname = "tmp-test.h5";
  unlink(fname);
  using namespace FileWriter;
  HDFFile f1;
  std::vector<StreamHDFInfo> stream_hdf_info;
  f1.init("tmp-test.h5", rapidjson::Value(), stream_hdf_info);
}

class T_CommandHandler : public testing::Test {
public:
  static void new_03() {
    using namespace FileWriter;
    auto cmd = gulp("tests/msg-cmd-new-03.json");
    LOG(7, "cmd: {:.{}}", cmd.data(), cmd.size());
    rapidjson::Document d;
    d.Parse(cmd.data(), cmd.size());
    char const *fname = d["file_attributes"]["file_name"].GetString();
    unlink(fname);
    MainOpt main_opt;
    FileWriter::CommandHandler ch(main_opt, nullptr);
    ch.handle({cmd.data(), cmd.size()});
  }

  static bool check_cue(std::vector<uint64_t> const &event_time_zero,
                        std::vector<uint32_t> const &event_index,
                        uint64_t cue_timestamp_zero, uint32_t cue_index) {
    bool found = false;
    size_t i2 = 0;
    for (auto &evt : event_time_zero) {
      if (evt == cue_timestamp_zero) {
        found = true;
        break;
      }
      ++i2;
    }
    if (!found) {
      return false;
    }
    return event_index[i2] == cue_index;
  }

  static void create_static_dataset() {
    using namespace FileWriter;
    using std::array;
    using std::vector;
    using std::string;
    MainOpt &main_opt = *g_main_opt.load();
    {
      rapidjson::Document cfg;
      cfg.Parse(R""({})"");
      main_opt.config_file = merge(cfg, main_opt.config_file);
    }
    rapidjson::Document json_command;
    {
      using namespace rapidjson;
      auto &j = json_command;
      auto &a = j.GetAllocator();
      j.SetObject();
      Value nexus_structure;
      nexus_structure.SetObject();

      Value children;
      children.SetArray();

      {
        Value g1;
        g1.SetObject();
        g1.AddMember("type", "group", a);
        g1.AddMember("name", "some_group", a);
        Value attr;
        attr.SetObject();
        attr.AddMember("NX_class", "NXinstrument", a);
        g1.AddMember("attributes", attr, a);
        Value ch;
        ch.SetArray();
        {
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "value",
                  "values": 42.24,
                  "attributes": {"units":"degree"}
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "more_complex_set",
                  "dataset": {
                    "space": "simple",
                    "type": "double",
                    "size": ["unlimited", 2]
                  },
                  "values": [
                    [13.1, 14]
                  ]
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "string_scalar",
                  "dataset": {
                    "type": "string"
                  },
                  "values": "the-scalar-string"
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "string_1d",
                  "dataset": {
                    "type": "string",
                    "size": ["unlimited"]
                  },
                  "values": ["the-scalar-string", "another-one"]
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "string_2d",
                  "dataset": {
                    "type": "string",
                    "size": ["unlimited", 2]
                  },
                  "values": [
                    ["the-scalar-string", "another-one"],
                    ["string_1_0", "string_1_1"]
                  ]
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "string_3d",
                  "dataset": {
                    "type": "string",
                    "size": ["unlimited", 3, 2]
                  },
                  "values": [
                    [
                      ["string_0_0_0", "string_0_0_1"],
                      ["string_0_1_0", "string_0_1_1"],
                      ["string_0_2_0", "string_0_2_1"]
                    ],
                    [
                      ["string_1_0_0", "string_1_0_1"],
                      ["string_1_1_0", "string_1_1_1"],
                      ["string_1_2_0", "string_1_2_1"]
                    ]
                  ]
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({
                  "type": "dataset",
                  "name": "string_fixed_1d",
                  "dataset": {
                    "type":"string",
                    "string_size": 32,
                    "size": ["unlimited"]
                  },
                  "values": ["the-scalar-string", "another-one"]
                })"");
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
          {
            Document jd;
            jd.Parse(
                R""({"type":"dataset", "name": "big_set", "dataset": {"space":"simple", "type":"double", "size":["unlimited", 4, 2]}})"");
            {
              Value values;
              values.SetArray();
              for (size_t i1 = 0; i1 < 7; ++i1) {
                Value v1;
                v1.SetArray();
                for (size_t i2 = 0; i2 < 4; ++i2) {
                  Value v2;
                  v2.SetArray();
                  for (size_t i3 = 0; i3 < 2; ++i3) {
                    v2.PushBack(Value(1000 * i1 + 10 * i2 + i3), a);
                  }
                  v1.PushBack(v2, a);
                }
                values.PushBack(v1, a);
              }
              jd.AddMember("values", values, a);
            }
            ch.PushBack(Value().CopyFrom(jd, a), a);
          }
        }
        g1.AddMember("children", ch, a);
        children.PushBack(g1, a);
      }
      nexus_structure.AddMember("children", children, a);
      j.AddMember("nexus_structure", nexus_structure, a);
      {
        Value v;
        v.SetObject();
        v.AddMember("file_name", StringRef("tmp-static-dataset.h5"), a);
        j.AddMember("file_attributes", v, a);
      }
      j.AddMember("cmd", StringRef("FileWriter_new"), a);
      j.AddMember("job_id", StringRef("000000000dataset"), a);
    }

    auto cmd = json_to_string(json_command);
    auto fname = get_string(&json_command, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), 8);

    FileWriter::CommandHandler ch(main_opt, nullptr);
    Msg msg;
    msg.data = (char *)cmd.data();
    msg.size = cmd.size();
    ch.handle(msg);
    ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)1);
    {
      string cmd("{\"recv_type\":\"FileWriter\", "
                 "\"cmd\":\"file_writer_tasks_clear_all\", "
                 "\"job_id\":\"000000000dataset\" }");
      ch.handle({(char *)cmd.data(), cmd.size()});
    }

    // Verification
    auto fid = H5Fopen(string(fname).c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    ASSERT_GE(fid, 0);
    auto g1 = H5Gopen2(fid, "some_group", H5P_DEFAULT);
    ASSERT_GE(g1, 0);
    auto ds = H5Dopen2(g1, "value", H5P_DEFAULT);
    ASSERT_GE(ds, 0);
    ASSERT_GT(H5Tequal(H5Dget_type(ds), H5T_NATIVE_DOUBLE), 0);
    auto attr = H5Aopen(ds, "units", H5P_DEFAULT);
    ASSERT_GE(attr, 0);
    H5Aclose(attr);
    H5Dclose(ds);
    H5Gclose(g1);
    H5Fclose(fid);
  }

  /// Can supply pre-generated test data for a source on a topic to profile
  /// the writing.
  class SourceDataGen {
  public:
    string topic;
    string source;
    uint64_t seed = 0;
    std::mt19937 rnd;
    vector<FlatBufs::ev42::fb> fbs;
    vector<FileWriter::Msg> msgs;
    // Number of messages already fed into file writer during testing
    size_t n_fed = 0;
    /// Generates n test messages which we can later feed from memory into the
    /// file writer.
    void pregenerate(int n, int n_events_per_message) {
      LOG(7, "generating {} {}...", topic, source);
      FlatBufs::ev42::synth synth(source, seed);
      rnd.seed(seed);
      for (int i1 = 0; i1 < n; ++i1) {
        // Number of events per message:
        // size_t n_ele = rnd() >> 24;
        // Currently fixed, have to adapt verification code first.
        auto n_ele = n_events_per_message;
        fbs.push_back(synth.next(n_ele));
        auto &fb = fbs.back();
        msgs.push_back(FileWriter::Msg{(char *)fb.builder->GetBufferPointer(),
                                       fb.builder->GetSize()});
      }
    }
  };

  static int recreate_file(rapidjson::Value *json_command) {
    // now try to recreate the file for testing:
    auto m = json_command->FindMember("file_attributes");
    auto fn = m->value.GetObject().FindMember("file_name")->value.GetString();
    auto x = H5Fcreate(fn, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    if (x < 0) {
      return -1;
    }
    H5Fclose(x);
    return 0;
  }

  static void data_ev42() {
    using namespace FileWriter;
    using std::array;
    using std::vector;
    using std::string;
    MainOpt &main_opt = *g_main_opt.load();
    bool do_verification = true;

    // Defaults such that the test has a chance to succeed
    {
      rapidjson::Document cfg;
      cfg.Parse(R""(
      {
        "nexus": {
          "indices": {
            "index_every_kb": 1
          },
          "chunk": {
            "chunk_n_elements": 64
          }
        },
        "unit_test": {
          "n_events_per_message": 32,
          "n_msgs_per_source": 128,
          "n_sources": 8,
          "n_msgs_per_batch": 1
        }
      })"");
      main_opt.config_file = merge(cfg, main_opt.config_file);
    }

    if (auto x =
            get_int(&main_opt.config_file, "unit_test.hdf.do_verification")) {
      do_verification = x.v == 1;
      LOG(4, "do_verification: {}", do_verification);
    }

    int n_msgs_per_source = 1;
    if (auto x =
            get_int(&main_opt.config_file, "unit_test.n_msgs_per_source")) {
      LOG(4, "unit_test.n_msgs_per_source: {}", x.v);
      n_msgs_per_source = x.v;
    }

    int n_sources = 1;
    if (auto x = get_int(&main_opt.config_file, "unit_test.n_sources")) {
      LOG(4, "unit_test.n_sources: {}", x.v);
      n_sources = x.v;
    }

    int n_events_per_message = 1;
    if (auto x =
            get_int(&main_opt.config_file, "unit_test.n_events_per_message")) {
      LOG(4, "unit_test.n_events_per_message: {}", x.v);
      n_events_per_message = x.v;
    }

    int n_msgs_per_batch = 1;
    if (auto x = get_int(&main_opt.config_file, "unit_test.n_msgs_per_batch")) {
      LOG(4, "unit_test.n_msgs_per_batch: {}", x.v);
      n_msgs_per_batch = x.v;
    }

    vector<SourceDataGen> sources;
    for (int i1 = 0; i1 < n_sources; ++i1) {
      sources.emplace_back();
      auto &s = sources.back();
      // Currently, we assume only one topic!
      s.topic = "topic.with.multiple.sources";
      s.source = fmt::format("for_example_motor_{:04}", i1);
      s.pregenerate(n_msgs_per_source, n_events_per_message);
    }

    rapidjson::Document json_command;
    {
      using namespace rapidjson;
      auto &j = json_command;
      auto &a = j.GetAllocator();
      j.SetObject();
      Value nexus_structure;
      nexus_structure.SetObject();

      Value children;
      children.SetArray();

      {
        Value g1;
        g1.SetObject();
        g1.AddMember("type", "group", a);
        g1.AddMember("name", "some_group", a);
        Value attr;
        attr.SetObject();
        attr.AddMember("NX_class", "NXinstrument", a);
        g1.AddMember("attributes", attr, a);
        Value ch;
        ch.SetArray();
        g1.AddMember("children", ch, a);
        children.PushBack(g1, a);
      }

      auto json_stream = [&a](string source, string topic,
                              string module) -> Value {
        Value g1;
        g1.SetObject();
        g1.AddMember("type", "group", a);
        g1.AddMember("name", Value(source.c_str(), a), a);
        Value attr;
        attr.SetObject();
        attr.AddMember("NX_class", "NXinstrument", a);
        g1.AddMember("attributes", attr, a);
        Value ch;
        ch.SetArray();
        {
          auto &children = ch;
          Value ds1;
          ds1.SetObject();
          ds1.AddMember("type", "stream", a);
          Value attr;
          attr.SetObject();
          attr.AddMember("this_will_be_a_double", Value(0.123), a);
          attr.AddMember("this_will_be_a_int64", Value(123), a);
          ds1.AddMember("attributes", attr, a);
          Document cfg_nexus;
          cfg_nexus.Parse(R""(
            {
              "nexus": {
                "indices": {
                  "index_every_kb": 1
                },
                "chunk": {
                  "chunk_n_elements": 64
                }
              }
            }
          )"");
          Value stream;
          stream.CopyFrom(cfg_nexus, a);
          stream.AddMember("topic", Value(topic.c_str(), a), a);
          stream.AddMember("source", Value(source.c_str(), a), a);
          stream.AddMember("module", Value(module.c_str(), a), a);
          stream.AddMember("type", Value("uint32", a), a);
          ds1.AddMember("stream", stream, a);
          children.PushBack(ds1, a);
        }
        g1.AddMember("children", ch, a);
        return g1;
      };

      for (auto &source : sources) {
        children.PushBack(json_stream(source.source, source.topic, "ev42"), a);
      }
      nexus_structure.AddMember("children", children, a);
      j.AddMember("nexus_structure", nexus_structure, a);
      {
        Value v;
        v.SetObject();
        v.AddMember("file_name", StringRef("tmp-ev42.h5"), a);
        j.AddMember("file_attributes", v, a);
      }
      j.AddMember("cmd", StringRef("FileWriter_new"), a);
      j.AddMember("job_id", StringRef("000000000042"), a);
    }

    auto cmd = json_to_string(json_command);
    // LOG(4, "command: {}", cmd);

    auto &d = json_command;
    auto fname = get_string(&d, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), size_t{8});

    FileWriter::CommandHandler ch(main_opt, nullptr);

    using DT = uint32_t;
    int const feed_msgs_times = 1;
    std::mt19937 rnd_nn;

    for (int file_i = 0; file_i < 1; ++file_i) {
      unlink(string(fname).c_str());

      Msg msg;
      msg.data = (char *)cmd.data();
      msg.size = cmd.size();
      ch.handle(msg);
      ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)1);

      auto &fwt = ch.file_writer_tasks.at(0);
      ASSERT_EQ(fwt->demuxers().size(), (size_t)1);

      LOG(6, "processing...");
      using CLK = std::chrono::steady_clock;
      using MS = std::chrono::milliseconds;
      auto t1 = CLK::now();
      for (;;) {
        bool change = false;
        for (int i1 = 0; i1 < feed_msgs_times; ++i1) {
          for (auto &source : sources) {
            for (int i2 = 0;
                 i2 < n_msgs_per_batch && source.n_fed < source.msgs.size();
                 ++i2) {
              auto &msg = source.msgs[source.n_fed];
              if (false) {
                auto v = binary_to_hex(msg.data, msg.size);
                LOG(7, "msg:\n{:.{}}", v.data(), v.size());
              }
              fwt->demuxers().at(0).process_message(msg.data, msg.size);
              source.n_fed++;
              change = true;
            }
          }
        }
        if (!change) {
          break;
        }
      }
      auto t2 = CLK::now();
      LOG(6, "processing done in {} ms", duration_cast<MS>(t2 - t1).count());
      LOG(6, "finishing...");
      {
        string cmd("{\"recv_type\":\"FileWriter\", "
                   "\"cmd\":\"file_writer_tasks_clear_all\"}");
        ch.handle({(char *)cmd.data(), cmd.size()});
      }
      auto t3 = CLK::now();
      LOG(6, "finishing done in {} ms", duration_cast<MS>(t3 - t2).count());
      LOG(6, "done in total {} ms", duration_cast<MS>(t3 - t1).count());
    }

    if (!do_verification) {
      return;
    }

    size_t minimum_expected_entries_in_the_index = 1;

    herr_t err;

    auto fid = H5Fopen(string(fname).c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    ASSERT_GE(fid, 0);

    vector<DT> data((size_t)32000);

    for (auto &source : sources) {
      string base_path = "/";
      string group_path = base_path + source.source;

      auto g0 = H5Gopen(fid, group_path.c_str(), H5P_DEFAULT);
      ASSERT_GE(g0, 0);
      H5Gclose(g0);

      auto ds = H5Dopen2(fid, (group_path + "/event_id").c_str(), H5P_DEFAULT);
      ASSERT_GE(ds, 0);
      ASSERT_GT(H5Iis_valid(ds), 0);

      auto dt = H5Dget_type(ds);
      ASSERT_GT(dt, 0);
      ASSERT_EQ(H5Tget_size(dt), sizeof(DT));

      auto dsp = H5Dget_space(ds);
      ASSERT_GT(dsp, 0);
      ASSERT_EQ(H5Sis_simple(dsp), 1);

      using A = array<hsize_t, 1>;
      A sini = {{(hsize_t)n_events_per_message}};
      A smax = {{(hsize_t)n_events_per_message}};
      A count = {{(hsize_t)n_events_per_message}};
      A start0 = {{(hsize_t)0}};
      auto mem = H5Screate(H5S_SIMPLE);
      err = H5Sset_extent_simple(mem, sini.size(), sini.data(), smax.data());
      ASSERT_GE(err, 0);
      err = H5Sselect_hyperslab(mem, H5S_SELECT_SET, start0.data(), nullptr,
                                count.data(), nullptr);
      ASSERT_GE(err, 0);

      // LOG(4, "have {} messages", source.msgs.size());
      for (size_t msg_i = 0; msg_i < source.msgs.size(); ++msg_i) {
        auto &fb = source.fbs.at(msg_i);
        A start = {{(hsize_t)msg_i * n_events_per_message}};
        err = H5Sselect_hyperslab(dsp, H5S_SELECT_SET, start.data(), nullptr,
                                  count.data(), nullptr);
        ASSERT_GE(err, 0);
        err = H5Dread(ds, nat_type<DT>(), mem, dsp, H5P_DEFAULT, data.data());
        ASSERT_GE(err, 0);
        auto fbd = fb.root()->detector_id();
        for (int i1 = 0; i1 < n_events_per_message; ++i1) {
          // LOG(4, "found: {:4}  {:6} vs {:6}", i1, data.at(i1),
          ASSERT_EQ(data.at(i1), fbd->Get(i1));
        }
      }

      H5Sclose(mem);
      H5Sclose(dsp);

      auto ds_cue_timestamp_zero = H5Dopen2(
          fid, (group_path + "/cue_timestamp_zero").c_str(), H5P_DEFAULT);
      ASSERT_GE(ds_cue_timestamp_zero, 0);
      ASSERT_GT(H5Iis_valid(ds_cue_timestamp_zero), 0);
      vector<uint64_t> cue_timestamp_zero(
          H5Sget_simple_extent_npoints(H5Dget_space(ds_cue_timestamp_zero)));
      err = H5Dread(ds_cue_timestamp_zero, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL,
                    H5P_DEFAULT, cue_timestamp_zero.data());
      ASSERT_EQ(err, 0);
      H5Dclose(ds_cue_timestamp_zero);

      auto ds_cue_index =
          H5Dopen2(fid, (group_path + "/cue_index").c_str(), H5P_DEFAULT);
      ASSERT_GE(ds_cue_index, 0);
      ASSERT_GT(H5Iis_valid(ds_cue_index), 0);
      vector<uint32_t> cue_index(
          H5Sget_simple_extent_npoints(H5Dget_space(ds_cue_index)));
      err = H5Dread(ds_cue_index, H5T_NATIVE_UINT32, H5S_ALL, H5S_ALL,
                    H5P_DEFAULT, cue_index.data());
      ASSERT_EQ(err, 0);
      H5Dclose(ds_cue_index);

      ASSERT_GE(cue_timestamp_zero.size(),
                minimum_expected_entries_in_the_index);
      ASSERT_EQ(cue_timestamp_zero.size(), cue_index.size());

      auto ds_event_time_zero =
          H5Dopen2(fid, (group_path + "/event_time_zero").c_str(), H5P_DEFAULT);
      ASSERT_GE(ds_event_time_zero, 0);
      ASSERT_GT(H5Iis_valid(ds_event_time_zero), 0);
      vector<uint64_t> event_time_zero(
          H5Sget_simple_extent_npoints(H5Dget_space(ds_event_time_zero)));
      err = H5Dread(ds_event_time_zero, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL,
                    H5P_DEFAULT, event_time_zero.data());
      ASSERT_EQ(err, 0);
      H5Dclose(ds_event_time_zero);

      auto ds_event_index =
          H5Dopen2(fid, (group_path + "/event_index").c_str(), H5P_DEFAULT);
      ASSERT_GE(ds_event_index, 0);
      ASSERT_GT(H5Iis_valid(ds_event_index), 0);
      vector<uint32_t> event_index(
          H5Sget_simple_extent_npoints(H5Dget_space(ds_event_index)));
      err = H5Dread(ds_event_index, H5T_NATIVE_UINT32, H5S_ALL, H5S_ALL,
                    H5P_DEFAULT, event_index.data());
      ASSERT_EQ(err, 0);
      H5Dclose(ds_event_index);

      ASSERT_GT(event_time_zero.size(), 0u);
      ASSERT_EQ(event_time_zero.size(), event_index.size());

      for (hsize_t i1 = 0; i1 < cue_timestamp_zero.size(); ++i1) {
        auto ok = check_cue(event_time_zero, event_index,
                            cue_timestamp_zero[i1], cue_index[i1]);
        ASSERT_TRUE(ok);
      }

      H5Tclose(dt);
      H5Dclose(ds);
    }

    H5Fclose(fid);

    ASSERT_EQ(recreate_file(&json_command), 0);
  }

  /// Can supply pre-generated test data for a source on a topic to profile
  /// the writing.
  class SourceDataGen_f142 {
  public:
    string topic;
    string source;
    uint64_t seed = 0;
    std::mt19937 rnd;
    vector<FlatBufs::f142::fb> fbs;
    vector<FileWriter::Msg> msgs;
    // Number of messages already fed into file writer during testing
    size_t n_fed = 0;
    /// Generates n test messages which we can later feed from memory into the
    /// file writer.
    void pregenerate(size_t array_size, uint64_t n) {
      LOG(7, "generating {} {}...", topic, source);
      auto ty = FlatBufs::f142::Value::Double;
      if (array_size > 0) {
        ty = FlatBufs::f142::Value::ArrayFloat;
      }
      FlatBufs::f142::synth synth(source, ty, int(array_size));
      rnd.seed(seed);
      for (uint64_t i1 = 0; i1 < n; ++i1) {
        // Number of events per message:
        // size_t n_ele = rnd() >> 24;
        // Currently fixed, have to adapt verification code first.
        fbs.push_back(synth.next(i1));
        auto &fb = fbs.back();
        msgs.push_back(FileWriter::Msg{(char *)fb.builder->GetBufferPointer(),
                                       fb.builder->GetSize()});
      }
    }
  };

  static void data_f142() {
    using namespace FileWriter;
    using std::array;
    using std::vector;
    using std::string;
    MainOpt &main_opt = *g_main_opt.load();
    bool do_verification = true;

    // Defaults such that the test has a chance to succeed
    {
      rapidjson::Document cfg;
      cfg.Parse(R""(
      {
        "nexus": {
          "chunk": {
            "chunk_n_elements": 64
          }
        },
        "unit_test": {
          "n_events_per_message": 32,
          "n_msgs_per_source": 128,
          "n_sources": 1,
          "n_msgs_per_batch": 1
        }
      })"");
      main_opt.config_file = merge(cfg, main_opt.config_file);
    }

    if (auto x =
            get_int(&main_opt.config_file, "unit_test.hdf.do_verification")) {
      do_verification = x.v == 1;
      LOG(4, "do_verification: {}", do_verification);
    }

    int n_msgs_per_source = 1;
    if (auto x =
            get_int(&main_opt.config_file, "unit_test.n_msgs_per_source")) {
      LOG(4, "unit_test.n_msgs_per_source: {}", x.v);
      n_msgs_per_source = int(x.v);
    }

    int n_sources = 1;
    if (auto x = get_int(&main_opt.config_file, "unit_test.n_sources")) {
      LOG(4, "unit_test.n_sources: {}", x.v);
      n_sources = int(x.v);
    }

    int n_msgs_per_batch = 1;
    if (auto x = get_int(&main_opt.config_file, "unit_test.n_msgs_per_batch")) {
      LOG(4, "unit_test.n_msgs_per_batch: {}", x.v);
      n_msgs_per_batch = int(x.v);
    }

    size_t array_size = 4;
    vector<SourceDataGen_f142> sources;
    for (int i1 = 0; i1 < n_sources; ++i1) {
      sources.emplace_back();
      auto &s = sources.back();
      // Currently, we assume only one topic!
      s.topic = "topic.with.multiple.sources";
      s.source = fmt::format("for_example_motor_{:04}", i1);
      s.pregenerate(array_size, n_msgs_per_source);
    }

    if (false) {
      for (auto &source : sources) {
        LOG(4, "msgs: {}  {}", source.source, source.msgs.size());
      }
    }

    rapidjson::Document json_command;
    {
      using namespace rapidjson;
      auto &j = json_command;
      auto &a = j.GetAllocator();
      j.SetObject();
      Value nexus_structure;
      nexus_structure.SetObject();

      Value children;
      children.SetArray();

      {
        Value g1;
        g1.SetObject();
        g1.AddMember("type", "group", a);
        g1.AddMember("name", "some_group", a);
        Value attr;
        attr.SetObject();
        attr.AddMember("NX_class", "NXinstrument", a);
        g1.AddMember("attributes", attr, a);
        Value ch;
        ch.SetArray();
        {
          auto &children = ch;
          Value ds1;
          ds1.SetObject();
          ds1.AddMember("type", "dataset", a);
          ds1.AddMember("name", "created_by_filewriter", a);
          Value attr;
          attr.SetObject();
          attr.AddMember("NX_class", "NXdetector", a);
          attr.AddMember("this_will_be_a_double", Value(0.123), a);
          attr.AddMember("this_will_be_a_int64", Value(123), a);
          ds1.AddMember("attributes", attr, a);
          Value dataset;
          dataset.SetObject();
          dataset.AddMember("space", "simple", a);
          dataset.AddMember("type", "uint64", a);
          Value dataset_size;
          dataset_size.SetArray();
          dataset_size.PushBack("unlimited", a);
          dataset_size.PushBack(Value(4), a);
          dataset_size.PushBack(Value(2), a);
          dataset.AddMember("size", dataset_size, a);
          ds1.AddMember("dataset", dataset, a);
          children.PushBack(ds1, a);
        }
        g1.AddMember("children", ch, a);
        children.PushBack(g1, a);
      }

      auto json_stream = [&a, array_size](string source, string topic,
                                          string module) -> Value {
        Value g1;
        g1.SetObject();
        g1.AddMember("type", "group", a);
        g1.AddMember("name", Value(source.c_str(), a), a);
        Value attr;
        attr.SetObject();
        attr.AddMember("NX_class", "NXinstrument", a);
        g1.AddMember("attributes", attr, a);
        Value ch;
        ch.SetArray();
        {
          auto &children = ch;
          Value ds1;
          ds1.SetObject();
          ds1.AddMember("type", "stream", a);
          Value attr;
          attr.SetObject();
          attr.AddMember("this_will_be_a_double", Value(0.123), a);
          attr.AddMember("this_will_be_a_int64", Value(123), a);
          ds1.AddMember("attributes", attr, a);
          Document cfg_nexus;
          cfg_nexus.Parse(R""(
            {
              "nexus": {
                "indices": {
                  "index_every_mb": 1
                },
                "chunk": {
                  "chunk_n_elements": 64
                }
              }
            }
          )"");
          Value stream;
          stream.CopyFrom(cfg_nexus, a);
          stream.AddMember("topic", Value(topic.c_str(), a), a);
          stream.AddMember("source", Value(source.c_str(), a), a);
          stream.AddMember("module", Value(module.c_str(), a), a);
          if (array_size == 0) {
            stream.AddMember("type", Value("double", a), a);
          } else {
            stream.AddMember("type", Value("float", a), a);
            stream.AddMember("array_size", Value().SetInt(array_size), a);
          }
          ds1.AddMember("stream", stream, a);
          children.PushBack(ds1, a);
        }
        g1.AddMember("children", ch, a);
        return g1;
      };

      for (auto &source : sources) {
        children.PushBack(json_stream(source.source, source.topic, "f142"), a);
      }
      {
        Document d;
        d.Parse(
            R"({"type":"group", "name":"a-subgroup", "children":[{"type":"group","name":"another-subgroup"}]})");
        children.PushBack(Value().CopyFrom(d, a), a);
      }
      nexus_structure.AddMember("children", children, a);
      j.AddMember("nexus_structure", nexus_structure, a);
      {
        Value v;
        v.SetObject();
        v.AddMember("file_name", StringRef("tmp-f142.h5"), a);
        j.AddMember("file_attributes", v, a);
      }
      j.AddMember("cmd", StringRef("FileWriter_new"), a);
      j.AddMember("job_id", StringRef("0000000data_f142"), a);
    }

    auto cmd = json_to_string(json_command);
    // LOG(4, "command: {}", cmd);

    auto &d = json_command;
    auto fname = get_string(&d, "file_attributes.file_name");
    ASSERT_GT(fname.v.size(), 8);

    FileWriter::CommandHandler ch(main_opt, nullptr);

    int const feed_msgs_times = 1;
    std::mt19937 rnd_nn;

    for (int file_i = 0; file_i < 1; ++file_i) {
      unlink(string(fname).c_str());

      Msg msg;
      msg.data = (char *)cmd.data();
      msg.size = cmd.size();
      ch.handle(msg);
      ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)1);

      auto &fwt = ch.file_writer_tasks.at(0);
      ASSERT_EQ(fwt->demuxers().size(), (size_t)1);

      LOG(6, "processing...");
      using CLK = std::chrono::steady_clock;
      using MS = std::chrono::milliseconds;
      auto t1 = CLK::now();
      for (;;) {
        bool change = false;
        for (int i1 = 0; i1 < feed_msgs_times; ++i1) {
          for (auto &source : sources) {
            for (int i2 = 0;
                 i2 < n_msgs_per_batch && source.n_fed < source.msgs.size();
                 ++i2) {
              auto &msg = source.msgs[source.n_fed];
              if (false) {
                auto v = binary_to_hex(msg.data, msg.size);
                LOG(7, "msg:\n{:.{}}", v.data(), v.size());
              }
              fwt->demuxers().at(0).process_message(msg.data, msg.size);
              source.n_fed++;
              change = true;
            }
          }
        }
        if (!change) {
          break;
        }
      }
      auto t2 = CLK::now();
      LOG(6, "processing done in {} ms", duration_cast<MS>(t2 - t1).count());
      LOG(6, "finishing...");
      {
        string cmd("{\"recv_type\":\"FileWriter\", "
                   "\"cmd\":\"file_writer_tasks_clear_all\"}");
        ch.handle({(char *)cmd.data(), cmd.size()});
      }
      auto t3 = CLK::now();
      LOG(6, "finishing done in {} ms", duration_cast<MS>(t3 - t2).count());
      LOG(6, "done in total {} ms", duration_cast<MS>(t3 - t1).count());
    }
  }
};

TEST_F(T_CommandHandler, new_03) { T_CommandHandler::new_03(); }

TEST_F(T_CommandHandler, create_static_dataset) {
  T_CommandHandler::create_static_dataset();
}

TEST_F(T_CommandHandler, data_ev42) { T_CommandHandler::data_ev42(); }

TEST_F(T_CommandHandler, data_f142) { T_CommandHandler::data_f142(); }
