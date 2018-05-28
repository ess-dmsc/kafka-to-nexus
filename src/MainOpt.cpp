#include "MainOpt.h"
#include "helper.h"
#include "json.h"
#include "uri.h"
#include <iostream>

using uri::URI;

MainOpt::MainOpt() {
  service_id = fmt::format("kafka-to-nexus--host:{}--pid:{}",
                           gethostname_wrapper(), getpid_wrapper());
}

int MainOpt::parse_config_file() {
  if (config_filename.empty()) {
    LOG(Sev::Notice, "given config filename is empty");
    return -1;
  }
  auto jsontxt = gulp(config_filename);
  return parse_config_json(std::string(jsontxt.data(), jsontxt.size()));
}

int MainOpt::parse_config_json(std::string ConfigJSONString) {
  // Parse the JSON configuration and extract parameters.
  // Currently, these parameters take precedence over what is given on the
  // command line.
  using nlohmann::json;
  try {
    ConfigJSON = json::parse(ConfigJSONString);
  } catch (...) {
    return 1;
  }
  if (auto v = find<std::string>("command-uri", ConfigJSON)) {
    URI uri("//localhost:9092/kafka-to-nexus.command");
    uri.parse(v.inner());
    command_broker_uri = uri;
  }
  if (auto v = find<std::string>("status-uri", ConfigJSON)) {
    URI uri("//localhost:9092/kafka-to-nexus.status");
    uri.parse(v.inner());
    kafka_status_uri = uri;
    do_kafka_status = true;
  }
  if (auto v = find<uint64_t>("status-master-interval", ConfigJSON)) {
    status_master_interval = v.inner();
  }
  if (auto v = find<json>("stream-master", ConfigJSON)) {
    if (auto v2 = find<uint64_t>("topic-write-interval", v.inner())) {
      topic_write_duration = std::chrono::milliseconds(v2.inner());
    }
  }
  if (auto v = find<json>("streamer", ConfigJSON)) {
    StreamerConfiguration.setStreamerOptions(v.inner());
  }
  if (auto v = find<json>("kafka", ConfigJSON)) {
    StreamerConfiguration.setRdKafkaOptions(v.inner());
  }
  if (auto v = find<std::string>("hdf-output-prefix", ConfigJSON)) {
    hdf_output_prefix = v.inner();
  }
  if (auto v = find<json>("commands", ConfigJSON)) {
    for (auto const &Command : v.inner()) {
      commands_from_config_file.emplace_back(Command.dump());
    }
  }
  if (auto v = find<bool>("source_do_process_message", ConfigJSON)) {
    source_do_process_message = v.inner();
  }
  if (auto v = find<std::string>("service_id", ConfigJSON)) {
    service_id = v.inner();
  }
  return 0;
}

void setup_logger_from_options(MainOpt const &opt) {
  g_ServiceID = opt.service_id;
  if (!opt.kafka_gelf.empty()) {
    URI uri(opt.kafka_gelf);
    log_kafka_gelf_start(uri.host, uri.topic);
    LOG(Sev::Debug, "Enabled kafka_gelf: //{}/{}", uri.host, uri.topic);
  }

  if (!opt.graylog_logger_address.empty()) {
    fwd_graylog_logger_enable(opt.graylog_logger_address);
  }
}

std::atomic<MainOpt *> g_main_opt;
