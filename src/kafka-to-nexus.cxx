#include <cstdlib>
#include <cstdio>
#include <string>
#include <csignal>
#include "logger.h"
#include "kafka-to-nexus.h"
#include "MainOpt.h"

void signal_handler(int signal) {
	LOG(0, "SIGNAL {}", signal);
	if (auto opt = g_main_opt.load()) {
		if (auto m = opt->master.load()) {
			m->stop();
		}
	}
}


int main(int argc, char ** argv) {
	auto po = parse_opt(argc, argv);
	if (po.first) {
		return 1;
	}
	auto opt = std::move(po.second);
	std::signal(SIGINT, signal_handler);
	std::signal(SIGTERM, signal_handler);

	printf("kafka-to-nexus-0.0.1 %.7s (ESS, BrightnESS)\n", GIT_COMMIT);
	printf("  Contact: dominik.werder@psi.ch, michele.brambilla@psi.ch\n\n");

	if (opt->help) {
		printf("Forwards EPICS process variables to Kafka topics.\n"
		       "Controlled via JSON packets sent over the configuration topic.\n"
		       "\n"
		       "\n"
		       "kafka-to-nexus\n"
		       "  --help, -h\n"
		       "\n"
		       "  --broker-command            <//host[:port][/topic]>\n"
		       "      Kafka brokers to connect with for configuration updates.\n"
		       "      Default: //%s/%s\n"
		       "\n",
			opt->master_config.command_listener.broker.host_port.c_str(),
			opt->master_config.command_listener.broker.topic.c_str()
		);

		printf("  --kafka-gelf                <//host[:port]/topic>\n"
		       "      Log to Graylog via Kafka GELF adapter.\n"
		       "\n");

		printf("  --graylog-logger-address    <host:port>\n"
		       "      Log to Graylog via graylog_logger library.\n"
		       "\n");

		if (false) {
			// probably removed soon...
		printf("  --assets-dir                <path>\n"
		       "      Path where program can find some supplementary files.\n"
		       "      Should point e.g. to the build or install directory.\n"
		       "      Default: %s\n"
		       "\n",
			opt->master_config.dir_assets.c_str());
		}

		printf("  -v\n"
		       "      Increase verbosity\n"
		       "\n");
		return 1;
	}

	setup_logger_from_options(*opt);

	BrightnESS::FileWriter::Master m(opt->master_config);
	opt->master = &m;
	std::thread t1([&m]{
		m.run();
	});
	t1.join();
	opt->master = nullptr;

	return 0;
}
