#include "Master.h"
#include "CommandHandler.h"
#include "logger.h"
#include <chrono>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <sys/types.h>
#include <unistd.h>

namespace FileWriter {

using std::vector;
using std::string;

std::string &CmdMsg_K::str() { return _str; }

Master::Master(MainOpt &config) : config(config), command_listener(config) {
  std::vector<char> buffer;
  buffer.resize(128);
  gethostname(buffer.data(), buffer.size());
  if (buffer.back() != 0) {
    // likely an error
    buffer.back() = 0;
    LOG(4, "Hostname got truncated: {}", buffer.data());
  }
  std::string hostname(buffer.data());
  file_writer_process_id_ =
      fmt::format("kafka-to-nexus--{}--{}", hostname, getpid());
  LOG(6, "file_writer_process_id: {}", file_writer_process_id());
}

void Master::handle_command_message(std::unique_ptr<KafkaW::Msg> &&msg) {
  CommandHandler command_handler(config, this);
  command_handler.handle({(char *)msg->data(), (int32_t)msg->size()});
}

void Master::handle_command(rapidjson::Document const &cmd) {
  CommandHandler command_handler(config, this);
  command_handler.handle(cmd);
}

void Master::run() {
  if (config.do_kafka_status) {
    LOG(3, "Publishing status to kafka://{}/{}",
        config.kafka_status_uri.host_port, config.kafka_status_uri.topic);
    KafkaW::BrokerOpt bopt;
    bopt.address = config.kafka_status_uri.host_port;
    auto producer = std::make_shared<KafkaW::Producer>(bopt);
    status_producer = std::make_shared<KafkaW::ProducerTopic>(
        producer, config.kafka_status_uri.topic);
  }
  for (auto const &cmd : config.commands_from_config_file) {
    this->handle_command(cmd);
  }
  command_listener.start();
  using Clock = std::chrono::steady_clock;
  auto t_last_statistics = Clock::now();
  while (do_run) {
    LOG(7, "Master poll");
    auto p = command_listener.poll();
    if (auto msg = p.is_Msg()) {
      LOG(7, "Handle a command");
      this->handle_command_message(std::move(msg));
    }
    if (config.do_kafka_status &&
        Clock::now() - t_last_statistics >
            std::chrono::milliseconds(config.status_master_interval)) {
      t_last_statistics = Clock::now();
      statistics();
    }
  }
  LOG(6, "calling stop on all stream_masters");
  for (auto &x : stream_masters) {
    x->stop();
  }
  LOG(6, "called stop on all stream_masters");
}

void Master::statistics() {
  using namespace rapidjson;
  rapidjson::Document js_status;
  js_status.SetObject();
  auto &a = js_status.GetAllocator();
  Value js_files;
  js_files.SetObject();
  for (auto &stream_master : stream_masters) {
    auto fwt_id_str =
        fmt::format("{:016x}", stream_master->file_writer_task().id());
    auto fwt = stream_master->file_writer_task().stats(a);
    // TODO
    // Add status when DM-450 lands.
    fwt.AddMember(
        "status",
        StringRef("[to-be-filled-when-StreamMaster-makes-status-available]"),
        a);
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
