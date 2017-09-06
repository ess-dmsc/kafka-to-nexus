#include "MainOpt.h"
#include "helper.h"
#include "uri.h"
#include <getopt.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/schema.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

using uri::URI;

int MainOpt::parse_config_file(std::string fname) {
  if (fname.empty()) {
    LOG(3, "given config filename is empty");
    return -1;
  }
  auto jsontxt = gulp(fname);
  return parse_config_json(std::string(jsontxt.data(), jsontxt.size()));
}

int MainOpt::parse_config_json(std::string json) {
  using namespace rapidjson;
  // Parse the JSON configuration and extract parameters.
  // Currently, these parameters take precedence over what is given on the
  // command line.
  auto &d = config_file;
  d.Parse(json.data(), json.size());
  if (d.HasParseError()) {
    LOG(3, "configuration is not well formed");
    return -5;
  }
  {
    auto o = get_string(&d, "command-uri");
    if (o.found()) {
      URI uri("//localhost:9092/kafka-to-nexus.command");
      uri.parse(o.v);
      command_broker_uri = uri;
    }
  }
  if (auto o = get_string(&d, "status-uri")) {
    URI uri("//localhost:9092/kafka-to-nexus.status");
    uri.parse(o.v);
    kafka_status_uri = uri;
    do_kafka_status = true;
  }
  if (auto o = get_int(&d, "status-master-interval")) {
    status_master_interval = o.v;
  }
  if (auto o = get_object(d, "kafka")) {
    for (auto &m : o.v->GetObject()) {
      if (m.value.IsString()) {
        kafka[m.name.GetString()] = m.value.GetString();
      }
      if (m.value.IsInt()) {
        kafka[m.name.GetString()] = fmt::format("{}", m.value.GetInt());
      }
    }
  }
  if (auto a = get_array(d, "commands")) {
    for (auto &e : a.v->GetArray()) {
      Document js_command;
      js_command.CopyFrom(e, js_command.GetAllocator());
      commands_from_config_file.push_back(std::move(js_command));
    }
  }
  if (auto o = get_bool(&d, "source_do_process_message")) {
    source_do_process_message = o.v;
  }

  LOG(3, "mmap");
  shm = MMap::create("tmp-mmap", 5 * 1024 * 1024 * 1024);
  std::memset(shm->addr(), 'a', 1024);

  jm = Jemalloc::create(shm->addr(),
                        (void *)((uint8_t *)shm->addr() + shm->size()));

  /*
  auto m1 = (std::atomic<uint32_t> *) shm->addr();
  m1->store(97);
  while (m1->load() < 110) {
    while(m1->load() % 2 == 1) {
    }
    m1->store(m1->load() + 1);
  }

  MPI_Barrier(comm_all);
  LOG(3, "alloc init");
  auto a = jm->alloc(1 * 1024 * 1024);
  if (a < shm->addr() || a >= (void*)((uint8_t*)shm->addr() + shm->size())) {
    LOG(3, "error alloc out of range");
    exit(1);
  }
  */

  return 0;
}

/**
Parses the options using getopt and returns a MainOpt
*/
std::pair<int, std::unique_ptr<MainOpt>> parse_opt(int argc, char **argv) {
  std::pair<int, std::unique_ptr<MainOpt>> ret{
      0, std::unique_ptr<MainOpt>(new MainOpt)};
  auto &opt = ret.second;
  opt->master = nullptr;
  // For the signal handler
  g_main_opt.store(opt.get());
  static struct option long_options[] = {
      {"help", no_argument, nullptr, 'h'},
      {"config-file", required_argument, nullptr, 0},
      {"command-uri", required_argument, nullptr, 0},
      {"status-uri", required_argument, nullptr, 0},
      {"kafka-gelf", required_argument, nullptr, 0},
      {"graylog-logger-address", required_argument, nullptr, 0},
      {"use-signal-handler", required_argument, nullptr, 0},
      {"teamid", required_argument, nullptr, 0},
      {nullptr, 0, nullptr, 0},
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
      if (std::string("command-uri") == lname) {
        URI uri("//localhost:9092/kafka-to-nexus.command");
        uri.parse(optarg);
        opt->command_broker_uri = uri;
      }
      if (std::string("status-uri") == lname) {
        URI uri("//localhost:9092/kafka-to-nexus.status");
        uri.parse(optarg);
        opt->kafka_status_uri = uri;
        opt->do_kafka_status = true;
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
        opt->teamid = strtoul(optarg, nullptr, 0);
      }
      break;
    }
  }

  if (getopt_error) {
    LOG(2, "ERROR parsing command line options");
    opt->help = true;
    ret.first = 1;
  }

  return ret;
}

void setup_logger_from_options(MainOpt const &opt) {
  if (opt.kafka_gelf != "") {
    URI uri(opt.kafka_gelf);
    log_kafka_gelf_start(uri.host, uri.topic);
    LOG(4, "Enabled kafka_gelf: //{}/{}", uri.host, uri.topic);
  }

  if (opt.graylog_logger_address != "") {
    fwd_graylog_logger_enable(opt.graylog_logger_address);
  }
}

std::atomic<MainOpt *> g_main_opt;
