#include <cstdlib>
#include <cstdio>
#include <string>
#include <getopt.h>
#include "logger.h"
#include "KafkaW.h"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

// POD
struct MainOpt {
bool help = false;
bool verbose = false;
uint64_t teamid = 0;
KafkaW::BrokerOpt broker_opt;
std::string topic {"commandtopicname"};
std::string cmd;
};

std::string make_command(std::string broker, uint64_t teamid) {
	using namespace rapidjson;
	Document d;
	auto & a = d.GetAllocator();
	d.SetObject();
	d.AddMember("cmd", Value("FileWriter_new", a), a);
	d.AddMember("teamid", teamid, a);
	d.AddMember("broker", Value(broker.c_str(), a), a);
	d.AddMember("filename", Value(fmt::format("tmp-{}.h5", teamid).c_str(), a), a);
	Value sa;
	sa.SetArray();
	{
		Value st;
		st.SetObject();
		st.AddMember("broker", Value(broker.c_str(), a), a);
		st.AddMember("topic", Value("topic.with.multiple.sources", a), a);
		st.AddMember("source", Value("source-00", a), a);
		sa.PushBack(st, a);
	}
	d.AddMember("streams", sa, a);
	StringBuffer buf1;
	PrettyWriter<StringBuffer> wr(buf1);
	d.Accept(wr);
	return buf1.GetString();
}

std::string make_command_exit(std::string broker, uint64_t teamid) {
	using namespace rapidjson;
	Document d;
	auto & a = d.GetAllocator();
	d.SetObject();
	d.AddMember("cmd", Value("FileWriter_exit", a), a);
	d.AddMember("teamid", teamid, a);
	StringBuffer buf1;
	PrettyWriter<StringBuffer> wr(buf1);
	d.Accept(wr);
	return buf1.GetString();
}

extern "C" char const GIT_COMMIT[];

int main(int argc, char ** argv) {
	MainOpt opt;

	static struct option long_options[] = {
		{"help",                            no_argument,              0, 'h'},
		{"broker-command-address",          required_argument,        0,  0 },
		{"broker-command-topic",            required_argument,        0,  0 },
		{"teamid",                          required_argument,        0,  0 },
		{"cmd",                             required_argument,        0,  0 },
		{0, 0, 0, 0},
	};
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
				opt.broker_opt.address = optarg;
			}
			if (std::string("broker-command-topic") == lname) {
				opt.topic = optarg;
			}
			if (std::string("teamid") == lname) {
				opt.teamid = strtoul(optarg, nullptr, 0);
			}
			if (std::string("cmd") == lname) {
				opt.cmd = optarg;
			}
			break;
		}
	}

	if (getopt_error) {
		LOG(5, "ERROR parsing command line options");
		opt.help = true;
		return 1;
	}

	printf("send-command  %.7s\n", GIT_COMMIT);
	printf("  Contact: dominik.werder@psi.ch\n\n");

	if (opt.help) {
		printf("Send a command to kafka-to-nexus.\n"
		       "\n"
		       "kafka-to-nexus\n"
		       "  --help, -h\n"
		       "\n"
		       "  --broker-command-address    host:port,host:port,...\n"
		       "      Default: %s\n"
		       "\n",
			opt.broker_opt.address.c_str());

		printf("  --broker-command-topic      <topic-name>\n"
		       "      Default: %s\n"
		       "\n",
			opt.topic.c_str());

		printf("  -v\n"
		       "      Increase verbosity\n"
		       "\n");
		return 1;
	}

	KafkaW::Producer producer(opt.broker_opt);
	KafkaW::Producer::Topic pt(producer, "kafka-to-nexus.command");
	if (opt.cmd == "new") {
		auto m1 = make_command(opt.broker_opt.address, opt.teamid);
		LOG(3, "sending {}", m1);
		pt.produce((void*)m1.data(), m1.size(), nullptr, true);
	}
	if (opt.cmd == "exit") {
		auto m1 = make_command_exit(opt.broker_opt.address, opt.teamid);
		LOG(3, "sending {}", m1);
		pt.produce((void*)m1.data(), m1.size(), nullptr, true);
	}
	producer.poll_while_outq();

	return 0;
}
