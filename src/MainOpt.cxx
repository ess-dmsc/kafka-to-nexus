#include "MainOpt.h"
#include "uri.h"
#include <getopt.h>


/**
Parses the options using getopt and returns a MainOpt
*/
std::pair<int, std::unique_ptr<MainOpt>> parse_opt(int argc, char ** argv) {
	std::unique_ptr<MainOpt> opt(new MainOpt);
	opt->master = nullptr;
	// For the signal handler
	g_main_opt.store(opt.get());
	static struct option long_options[] = {
		{"help",                            no_argument,              0, 'h'},
		{"broker-command-address",          required_argument,        0,  0 },
		{"broker-command-topic",            required_argument,        0,  0 },
		{"kafka-gelf",                      required_argument,        0,  0 },
		{"graylog-logger-address",          required_argument,        0,  0 },
		{"teamid",                          required_argument,        0,  0 },
		{"assets-dir",                      required_argument,        0,  0 },
		{0, 0, 0, 0},
	};
	std::string cmd;
	int option_index = 0;
	bool getopt_error = false;
	while (true) {
		int c = getopt_long(argc, argv, "vh", long_options, &option_index);
		//LOG(2, "c getopt {}", c);
		if (c == -1) break;
		if (c == '?') {
			getopt_error = true;
		}
		switch (c) {
		case 'v':
			opt->verbose = true;
			log_level = std::min(9, log_level + 1);
			break;
		case 'h':
			opt->help = true;
			break;
		case 0:
			auto lname = long_options[option_index].name;
			if (std::string("help") == lname) {
				opt->help = true;
			}
			if (std::string("broker-command-address") == lname) {
				opt->master_config.command_listener.address = optarg;
			}
			if (std::string("broker-command-topic") == lname) {
				opt->master_config.command_listener.topic = optarg;
			}
			if (std::string("kafka-gelf") == lname) {
				opt->kafka_gelf = optarg;
			}
			if (std::string("graylog-logger-address") == lname) {
				opt->graylog_logger_address = optarg;
			}
			if (std::string("teamid") == lname) {
				opt->master_config.teamid = strtoul(optarg, nullptr, 0);
			}
			if (std::string("assets-dir") == lname) {
				opt->master_config.dir_assets = optarg;
			}
			break;
		}
	}

	if (getopt_error) {
		LOG(2, "ERROR parsing command line options");
		opt->help = true;
		return {1, std::move(opt)};
	}

	return {0, std::move(opt)};
}


void setup_logger_from_options(MainOpt const & opt) {
	if (opt.kafka_gelf != "") {
		BrightnESS::uri::URI uri(opt.kafka_gelf);
		log_kafka_gelf_start(uri.host, uri.topic);
		LOG(4, "Enabled kafka_gelf: //{}/{}", uri.host, uri.topic);
	}

	if (opt.graylog_logger_address != "") {
		fwd_graylog_logger_enable(opt.graylog_logger_address);
	}
}


std::atomic<MainOpt *> g_main_opt;
