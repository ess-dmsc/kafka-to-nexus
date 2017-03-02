#include "test-roundtrip.h"
#include <future>
#include "logger.h"
#include "helper.h"
#include "KafkaW.h"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <rapidjson/prettywriter.h>
#include "schemas/f141_epics_nt_generated.h"
#include <type_traits>

#if HAVE_GTEST
#include <gtest/gtest.h>
#endif

namespace BrightnESS {
namespace FileWriter {
namespace Test {

/// Produce a command from a json file
int64_t produce_command_from_file(CommandListenerConfig config, std::string file) {
	KafkaW::BrokerOpt opt;
	opt.address = config.address;
	KafkaW::Producer p(opt);
	std::promise<int64_t> offset;
	std::function<void(rd_kafka_message_t const * msg)> cb = [&offset](rd_kafka_message_t const * msg) {
		offset.set_value(msg->offset);
	};
	p.on_delivery_ok = &cb;
	KafkaW::Producer::Topic pt(p, config.topic);
	pt.do_copy();
	auto v1 = gulp(file.c_str());
	pt.produce(v1.data(), v1.size(), nullptr);
	p.poll_while_outq();
	auto fut = offset.get_future();
	auto x = fut.wait_for(std::chrono::milliseconds(2000));
	if (x == std::future_status::ready) {
		return fut.get();
	}
	LOG(0, "Timeout on production of test message");
	return -1;
}


template <typename T, typename = int>
struct _has_teamid : std::false_type {
static void fill(T & fwdinfo, uint64_t teamid) {
}
};
template <typename T>
struct _has_teamid <T, decltype((void) T::teamid, 0)> : std::true_type {
static void fill(T & fwdinfo, uint64_t teamid) {
	fwdinfo.teamid = teamid;
}
};



void roundtrip_simple_01(MainOpt & opt) {
	LOG(5, "Run test:  Test::roundtrip_simple_01");
	using namespace BrightnESS::FileWriter;
	using namespace rapidjson;
	using CLK = std::chrono::steady_clock;
	using MS = std::chrono::milliseconds;
	Master m(opt.master_config);
	opt.master = &m;
	auto fn_cmd = "test/msg-conf-new-01.json";
	auto of = produce_command_from_file(opt.master_config.command_listener, fn_cmd);
	opt.master_config.command_listener.start_at_command_offset = of - 1;
	std::thread t1([&m]{
		#if HAVE_GTEST
		ASSERT_NO_THROW( m.run() );
		#else
		m.run();
		#endif
	});

	// We want the streamers to be ready
	//stream_master.wait_until_connected();
	std::this_thread::sleep_for(MS(1000));

	auto json_data = gulp(fn_cmd);
	Document d;
	d.Parse(json_data.data(), json_data.size());
	std::vector<std::string> test_sourcenames;
	std::vector<std::string> test_topics;
	for (auto & x : d["streams"].GetArray()) {
		test_sourcenames.push_back(x["source"].GetString());
		test_topics.push_back(x["topic"].GetString());
	}

	{
		// Produce sample data using the nt types scheme only
		KafkaW::BrokerOpt opt;
		opt.address = "localhost:9092";
		//auto nowns = []{return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();};
		for (size_t i3 = 0; i3 < test_sourcenames.size(); ++i3) {
			KafkaW::Producer prod(opt);
			KafkaW::Producer::Topic topic(prod, test_topics[i3]);
			topic.do_copy();
			auto & sourcename = test_sourcenames[i3];
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
					data[i2] = 10000 * (i3+1) + 100 * i1 + i2;
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
				//epicspv.add_fwdinfo(&fi);
				FinishEpicsPVBuffer(builder, epicspv.Finish());
				if (true) {
					topic.produce(builder.GetBufferPointer(), builder.GetSize(), nullptr);
					prod.poll();
				}
			}
			prod.poll_while_outq();
		}
		//fwt->file_flush();
	}

	auto start = CLK::now();
	while (CLK::now() - start < MS(5000)) {
		std::this_thread::sleep_for(MS(200));
	}
	LOG(5, "Stop Master");
	m.stop();
	t1.join();
}

}
}
}
