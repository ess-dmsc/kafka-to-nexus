#include <cstdlib>
#include <cstdio>
#include <string>
#include <csignal>
#include <getopt.h>
#include "logger.h"
#include "kafka-to-nexus.h"

#if HAVE_GTEST
#include <gtest/gtest.h>
#include "test-roundtrip.h"
#endif

#if HAVE_GTEST
class Roundtrip : public ::testing::Test {
public:
static MainOpt * opt;
};
MainOpt * Roundtrip::opt = nullptr;
#endif

MainOpt * g_main_opt = nullptr;

void signal_handler(int signal) {
	LOG(9, "SIGNAL {}", signal);
	if (auto m = g_main_opt->master.load()) {
		m->stop();
	}
}

int main(int argc, char ** argv) {
	std::signal(SIGINT, signal_handler);
	std::signal(SIGTERM, signal_handler);
	MainOpt opt;
	g_main_opt = &opt;

	static struct option long_options[] = {
		{"help",                            no_argument,              0, 'h'},
		{"broker-command-address",          required_argument,        0,  0 },
		{"broker-command-topic",            required_argument,        0,  0 },
		{"teamid",                          required_argument,        0,  0 },
		{"test",                            no_argument,              0,  0 },
		{0, 0, 0, 0},
	};
	std::string cmd;
	int option_index = 0;
	bool getopt_error = false;
	while (true) {
		int c = getopt_long(argc, argv, "vh", long_options, &option_index);
		//LOG(5, "c getopt {}", c);
		if (c == -1) break;
		if (c == '?') {
			getopt_error = true;
		}
		switch (c) {
		case 'v':
			opt.verbose = true;
			log_level = std::max(0, log_level - 1);
			break;
		case 'h':
			opt.help = true;
			break;
		case 0:
			auto lname = long_options[option_index].name;
			if (std::string("help") == lname) {
				opt.help = true;
			}
			if (std::string("broker-command-address") == lname) {
				opt.master_config.command_listener.address = optarg;
			}
			if (std::string("broker-command-topic") == lname) {
				opt.master_config.command_listener.topic = optarg;
			}
			if (std::string("teamid") == lname) {
				opt.master_config.teamid = strtoul(optarg, nullptr, 0);
			}
			if (std::string("test") == lname) {
				opt.gtest = true;
			}
			break;
		}
	}

	if (getopt_error) {
		LOG(5, "ERROR parsing command line options");
		opt.help = true;
		return 1;
	}

	printf("kafka-to-nexus-0.0.1  (ESS, BrightnESS)  %.7s\n", GIT_COMMIT);
	printf("  Contact: dominik.werder@psi.ch, michele.brambilla@psi.ch\n\n");

	if (opt.help) {
		printf("Forwards EPICS process variables to Kafka topics.\n"
		       "Controlled via JSON packets sent over the configuration topic.\n"
		       "\n"
		       "\n"
		       "kafka-to-nexus\n"
		       "  --help, -h\n"
		       "\n"
		       "  --test\n"
		       "      Run tests\n"
		       "\n"
		       "  --broker-command-address    host:port,host:port,...\n"
		       "      Kafka brokers to connect with for configuration updates.\n"
		       "      Default: %s\n"
		       "\n",
			opt.master_config.command_listener.address.c_str());

		printf("  --broker-command-topic      <topic-name>\n"
		       "      Topic name to listen to for configuration updates.\n"
		       "      Default: %s\n"
		       "\n",
			opt.master_config.command_listener.topic.c_str());

		printf("  -v\n"
		       "      Increase verbosity\n"
		       "\n");
		printf("  --test\n"
		       "      Run test suite\n"
		       "\n");
		return 1;
	}

	if (opt.gtest) {
		#if HAVE_GTEST
		Roundtrip::opt = & opt;
		::testing::InitGoogleTest(&argc, argv);
		return RUN_ALL_TESTS();
		#else
		printf("ERROR To run tests, the executable must be compiled with the Google Test library.\n");
		return 1;
		#endif
	}

	Master m(opt.master_config);
	opt.master = &m;
	std::thread t1([&m]{
		m.run();
	});
	t1.join();
	opt.master = nullptr;

	return 0;
}

#if HAVE_GTEST
TEST(librdkafka, basics) {
	ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, 0);
}
TEST_F(Roundtrip, simple_01) {
	BrightnESS::FileWriter::Test::roundtrip_simple_01(*opt);
}
#endif
