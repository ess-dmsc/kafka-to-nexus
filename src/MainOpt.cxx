#include "MainOpt.h"
#include "helper.h"
#include "uri.h"
#include <getopt.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/schema.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

using BrightnESS::uri::URI;

int MainOpt::parse_config_file(std::string fname) {
  using namespace rapidjson;
  if (fname == "") {
    LOG(3, "given config filename is empty");
    return -1;
  }
  // Parse the JSON configuration and extract parameters.
  // Currently, these parameters take precedence over what is given on the
  // command line.
  auto jsontxt = gulp(fname);
  auto &d = config_file;
  d.Parse(jsontxt.data(), jsontxt.size());
  if (d.HasParseError()) {
    LOG(3, "configuration is not well formed");
    return -5;
  }
  return 0;
}

/**
Parses the options using getopt and returns a MainOpt
*/
std::pair<int, std::unique_ptr<MainOpt> > parse_opt(int argc, char **argv) {
  std::pair<int, std::unique_ptr<MainOpt> > ret{ 0, std::unique_ptr<MainOpt>(
                                                        new MainOpt) };
  auto &opt = ret.second;
  opt->master = nullptr;
  // For the signal handler
  g_main_opt.store(opt.get());
  static struct option long_options[] = {
    { "help", no_argument, 0, 'h' },
    { "config-file", required_argument, 0, 0 },
    { "broker-command", required_argument, 0, 0 },
    { "kafka-gelf", required_argument, 0, 0 },
    { "graylog-logger", required_argument, 0, 0 },
    { "use-signal-handler", required_argument, 0, 0 },
    { "teamid", required_argument, 0, 0 },
    { "assets-dir", required_argument, 0, 0 },
    { 0, 0, 0, 0 },
  };
  std::string cmd;
  int option_index = 0;
  bool getopt_error = false;
  while (true) {
    int c = getopt_long(argc, argv, "vh", long_options, &option_index);
    // LOG(2, "c getopt {}", c);
    if (c == -1)
      break;
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
      if (std::string("config-file") == lname) {
        if (opt->parse_config_file(optarg)) {
          opt->help = true;
          ret.first = 1;
        }
      }
      if (std::string("broker-command") == lname) {
        URI x(optarg);
        x.default_host("localhost");
        x.default_port(9092);
        x.default_path("kafka-to-nexus.command");
        opt->master_config.command_listener.broker = x;
      }
      if (std::string("kafka-gelf") == lname) {
        opt->kafka_gelf = optarg;
      }
      if (std::string("graylog-logger-address") == lname) {
        opt->graylog_logger_address = optarg;
      }
      if (std::string("use-signal-handler") == lname) {
        opt->use_signal_handler = (bool)strtoul(optarg, nullptr, 0);
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
    ret.first = 1;
  }

  opt->master_config.config_file = &opt->config_file;

  return ret;
}

void setup_logger_from_options(MainOpt const &opt) {
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
