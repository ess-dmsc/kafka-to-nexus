#include "MainOpt.h"
#include "helper.h"
#include "uri.h"
#include <getopt.h>
#include <iostream>
#include <rapidjson/prettywriter.h>
#include <rapidjson/schema.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

using uri::URI;

int MainOpt::parse_config_file() {
  if (config_filename.empty()) {
    LOG(Sev::Notice, "given config filename is empty");
    return -1;
  }
  auto jsontxt = gulp(config_filename);
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
    LOG(Sev::Notice, "configuration is not well formed");
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
  if (auto o = get_object(d, "stream-master")) {
    for (auto &m : o.v->GetObject()) {
      if (m.name.GetString() == std::string{"topic-write-interval"}) {
        topic_write_duration = std::chrono::milliseconds{m.value.GetUint64()};
      }
    }
  }
  if (auto o = get_object(d, "streamer")) {
    StreamerConfiguration.setStreamerOptions(o.v);
  }
  if (auto o = get_object(d, "kafka")) {
    StreamerConfiguration.setRdKafkaOptions(o.v);
  }
  if (auto o = get_string(&d, "hdf-output-prefix")) {
    hdf_output_prefix = o.v;
  }
  if (auto a = get_array(d, "commands")) {
    for (auto &e : a.v->GetArray()) {
      Document js_command;
      js_command.CopyFrom(e, js_command.GetAllocator());
      rapidjson::StringBuffer buf;
      rapidjson::Writer<StringBuffer> wr(buf);
      js_command.Accept(wr);
      commands_from_config_file.emplace_back(buf.GetString(), buf.GetSize());
    }
  }
  if (auto o = get_bool(&d, "source_do_process_message")) {
    source_do_process_message = o.v;
  }

  return 0;
}

void setup_logger_from_options(MainOpt const &opt) {
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
