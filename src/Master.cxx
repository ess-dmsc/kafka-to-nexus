#include "Master.h"
#include "CommandHandler.h"
#include "Msg.h"
#include "helper.h"
#include "logger.h"

#include <algorithm>
#include <chrono>

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <sys/types.h>
#include <unistd.h>

namespace FileWriter {

Master::Master(MainOpt &config) : config(config), command_listener(config) {
  std::vector<char> buffer;
  buffer.resize(128);
  gethostname(buffer.data(), buffer.size());
  if (buffer.back() != 0) {
    // likely an error
    buffer.back() = 0;
    LOG(Sev::Info, "Hostname got truncated: {}", buffer.data());
  }
  std::string hostname(buffer.data());
  file_writer_process_id_ =
      fmt::format("kafka-to-nexus--{}--{}", hostname, getpid_wrapper());
  LOG(Sev::Info, "file_writer_process_id: {}", file_writer_process_id());
}

void Master::handle_command_message(std::unique_ptr<KafkaW::Msg> &&msg) {
  CommandHandler command_handler(config, this);
  command_handler.handle(Msg::owned((char const *)msg->data(), msg->size()));
}

void Master::handle_command(std::string const &command) {
  CommandHandler command_handler(config, this);
  command_handler.tryToHandle(command);
}

void Master::run() {
  // Set up connection to the Kafka status topic if desired.
  if (config.do_kafka_status) {
    LOG(Sev::Info, "Publishing status to kafka://{}/{}",
        config.kafka_status_uri.host_port, config.kafka_status_uri.topic);
    KafkaW::BrokerSettings BrokerSettings;
    BrokerSettings.Address = config.kafka_status_uri.host_port;
    auto producer = std::make_shared<KafkaW::Producer>(BrokerSettings);
    try {
      status_producer = std::make_shared<KafkaW::ProducerTopic>(
          producer, config.kafka_status_uri.topic);
    } catch (KafkaW::TopicCreationError const &e) {
      LOG(Sev::Error, "Can not create Kafka status producer: {}", e.what());
    }
  }

  // Interpret commands given directly in the configuration file, useful
  // for testing.
  for (auto const &cmd : config.commands_from_config_file) {
    this->handle_command(cmd);
  }

  command_listener.start();
  using Clock = std::chrono::steady_clock;
  auto t_last_statistics = Clock::now();
  while (do_run) {
    LOG(Sev::Debug, "Master poll");
    auto p = command_listener.poll();
    if (auto msg = p.isMsg()) {
      LOG(Sev::Debug, "Handle a command");
      this->handle_command_message(std::move(msg));
    }
    if (config.do_kafka_status &&
        Clock::now() - t_last_statistics >
            std::chrono::milliseconds(config.status_master_interval)) {
      t_last_statistics = Clock::now();
      statistics();
    }

    // Remove any job which is in 'is_removable' state
    stream_masters.erase(
        std::remove_if(stream_masters.begin(), stream_masters.end(),
                       [](std::unique_ptr<StreamMaster<Streamer>> &Iter) {
                         return Iter->status() ==
                                Status::StreamMasterErrorCode::is_removable;
                       }),
        stream_masters.end());
  }
  LOG(Sev::Info, "calling stop on all stream_masters");
  for (auto &x : stream_masters) {
    x->stop();
  }
  LOG(Sev::Info, "called stop on all stream_masters");
}

void Master::statistics() {
  using namespace rapidjson;
  rapidjson::Document js_status;
  auto &a = js_status.GetAllocator();
  js_status.SetObject();
  js_status.AddMember("type", StringRef("filewriter_status_master"), a);
  Value js_files;
  js_files.SetObject();
  for (auto &stream_master : stream_masters) {
    auto fwt_id_str =
        fmt::format("{}", stream_master->getFileWriterTask().job_id());
    auto fwt = stream_master->getFileWriterTask().stats(a);
    js_files.AddMember(Value(fwt_id_str.c_str(), a), fwt, a);
  }
  js_status.AddMember("files", js_files, a);
  rapidjson::StringBuffer buffer;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
  writer.SetIndent(' ', 2);
  js_status.Accept(writer);
  if (status_producer) {
    status_producer->produce((KafkaW::uchar *)buffer.GetString(),
                             buffer.GetSize());
  }
}

void Master::stop() { do_run = false; }

std::string Master::file_writer_process_id() const {
  return file_writer_process_id_;
}

} // namespace FileWriter
