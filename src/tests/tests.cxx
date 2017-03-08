#include <gtest/gtest.h>
#include "../MainOpt.h"
#include "roundtrip.h"

int main(int argc, char ** argv) {
	auto po = parse_opt(argc, argv);
	if (po.first) {
		return 1;
	}
	auto opt = std::move(po.second);
	setup_logger_from_options(*opt);
	Roundtrip::opt = opt.get();
	::testing::InitGoogleTest(&argc, argv);
	auto & f = ::testing::GTEST_FLAG(filter);
	if (f.find("remote_kafka") == std::string::npos) {
		f = f + std::string(":-*remote_kafka*");
	}
	return RUN_ALL_TESTS();
}

static_assert(RD_KAFKA_RESP_ERR_NO_ERROR == 0, "Make sure that NO_ERROR is and stays 0");

#if 0
	if (false) {
		// test if log messages arrive on all destinations
		for (int i1 = 0; i1 < 100; ++i1) {
			LOG(i1 % 8, "Log ix {} level {}", i1, i1 % 8);
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		}
	}
#endif
