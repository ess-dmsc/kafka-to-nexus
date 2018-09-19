#include "Master.h"
#include "CommandHandler.h"
#include "Msg.h"
#include "helper.h"
#include "json.h"
#include "logger.h"
#include <algorithm>
#include <chrono>
#include <functional>
#include <sys/types.h>
#include <unistd.h>

namespace FileWriter {

Master::Master(MainOpt &MainOpt_)
    : command_listener(MainOpt_), MainOpt_(MainOpt_) {
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
  CommandHandler command_handler(getMainOpt(), this);
  command_handler.tryToHandle(
      Msg::owned((char const *)msg->data(), msg->size()));
}

void Master::handle_command(std::string const &command) {
  CommandHandler command_handler(getMainOpt(), this);
  command_handler.tryToHandle(command);
}

std::unique_ptr<StreamMaster<Streamer>> &
Master::getStreamMasterForJobID(std::string JobID) {
  for (auto &StreamMaster : StreamMasters) {
    if (StreamMaster->getJobId() == JobID) {
      return StreamMaster;
    }
  }
  static std::unique_ptr<StreamMaster<Streamer>> NotFound;
  return NotFound;
}

void Master::addStreamMaster(
    std::unique_ptr<StreamMaster<Streamer>> StreamMaster) {
  StreamMasters.push_back(std::move(StreamMaster));
}

struct OnScopeExit {
  OnScopeExit(std::function<void()> Action) : ExitAction(Action){};
  ~OnScopeExit() { ExitAction(); };
  std::function<void()> ExitAction;
};

void Master::run() {
  OnScopeExit SetExitFlag([this]() { HasExitedRunLoop = true; });
  // Set up connection to the Kafka status topic if desired.
  if (getMainOpt().do_kafka_status) {
    LOG(Sev::Info, "Publishing status to kafka://{}/{}",
        getMainOpt().kafka_status_uri.host_port,
        getMainOpt().kafka_status_uri.topic);
    KafkaW::BrokerSettings BrokerSettings;
    BrokerSettings.Address = getMainOpt().kafka_status_uri.host_port;
    auto producer = std::make_shared<KafkaW::Producer>(BrokerSettings);
    try {
      status_producer = std::make_shared<KafkaW::ProducerTopic>(
          producer, getMainOpt().kafka_status_uri.topic);
    } catch (KafkaW::TopicCreationError const &e) {
      LOG(Sev::Error, "Can not create Kafka status producer: {}", e.what());
    }
  }

  // Interpret commands given directly in the configuration file, useful
  // for testing.
  for (auto const &cmd : getMainOpt().commands_from_config_file) {
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
    if (getMainOpt().do_kafka_status &&
        Clock::now() - t_last_statistics >
            std::chrono::milliseconds(getMainOpt().status_master_interval)) {
      t_last_statistics = Clock::now();
      statistics();
    }

    // Remove any job which is in 'is_removable' state
    StreamMasters.erase(
        std::remove_if(StreamMasters.begin(), StreamMasters.end(),
                       [](std::unique_ptr<StreamMaster<Streamer>> &Iter) {
                         return Iter->status().isRemovable();
                       }),
        StreamMasters.end());
  }
  LOG(Sev::Info, "calling stop on all stream_masters");
  stopStreamMasters();
  LOG(Sev::Info, "called stop on all stream_masters");
}

void Master::stopStreamMasters() {
  for (auto &x : StreamMasters) {
    x->stop();
  }
}

void Master::statistics() {
  if (!status_producer) {
    return;
  }
  using nlohmann::json;
  auto Status = json::object();
  Status["type"] = "filewriter_status_master";
  Status["service_id"] = getMainOpt().service_id;
  Status["files"] = json::object();
  for (auto &StreamMaster : StreamMasters) {
    auto FilewriterTaskID =
        fmt::format("{}", StreamMaster->getFileWriterTask().job_id());
    auto FilewriterTaskStatus = StreamMaster->getFileWriterTask().stats();
    Status["files"][FilewriterTaskID] = FilewriterTaskStatus;
  }
  auto Buffer = Status.dump();
  status_producer->produce((KafkaW::uchar *)Buffer.data(), Buffer.size());
}

void Master::stop() { do_run = false; }

std::string Master::file_writer_process_id() const {
  return file_writer_process_id_;
}

MainOpt &Master::getMainOpt() { return MainOpt_; }

std::shared_ptr<KafkaW::ProducerTopic> Master::getStatusProducer() {
  return status_producer;
}

} // namespace FileWriter
