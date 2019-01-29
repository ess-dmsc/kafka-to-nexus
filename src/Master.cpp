#include "Master.h"
#include "CommandHandler.h"
#include "Errors.h"
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

Master::Master(MainOpt &Config) : Listener(Config), MainConfig(Config) {
  std::vector<char> buffer;
  buffer.resize(128);
  gethostname(buffer.data(), buffer.size());
  if (buffer.back() != 0) {
    // likely an error
    buffer.back() = 0;
    LOG(Sev::Info, "Hostname got truncated: {}", buffer.data());
  }
  std::string hostname(buffer.data());
  FileWriterProcessId =
      fmt::format("kafka-to-nexus--{}--{}", hostname, getpid_wrapper());
  LOG(Sev::Info, "getFileWriterProcessId: {}",
      Master::getFileWriterProcessId());
}

void Master::handle_command_message(std::unique_ptr<Msg> CommandMessage) {
  CommandHandler command_handler(getMainOpt(), this);
  if (CommandMessage->MetaData.TimestampType !=
      RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
    auto TempTimeStamp = CommandMessage->MetaData.Timestamp;
    command_handler.tryToHandle(std::move(CommandMessage),
                                TempTimeStamp);
  } else {
    command_handler.tryToHandle(std::move(CommandMessage));
  }
}

void Master::handle_command(std::string const &Command) {
  CommandHandler command_handler(getMainOpt(), this);
  command_handler.tryToHandle(Command);
}

std::unique_ptr<StreamMaster<Streamer>> &
Master::getStreamMasterForJobID(std::string const &JobID) {
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
  explicit OnScopeExit(std::function<void()> Action)
      : ExitAction(std::move(Action)){};
  ~OnScopeExit() {
    try {
      ExitAction();
    } catch (std::bad_function_call &Error) {
      LOG(Sev::Warning, "OnScopeExit::~OnScopeExit(): Failure to call.");
    }
  };
  std::function<void()> ExitAction;
};

void Master::run() {
  OnScopeExit SetExitFlag([this]() { HasExitedRunLoop = true; });
  // Set up connection to the Kafka status topic if desired.
  if (getMainOpt().ReportStatus) {
    LOG(Sev::Info, "Publishing status to kafka://{}/{}",
        getMainOpt().KafkaStatusURI.HostPort,
        getMainOpt().KafkaStatusURI.Topic);
    KafkaW::BrokerSettings BrokerSettings;
    BrokerSettings.Address = getMainOpt().KafkaStatusURI.HostPort;
    auto Producer = std::make_shared<KafkaW::Producer>(BrokerSettings);
    try {
      StatusProducer = std::make_shared<KafkaW::ProducerTopic>(
          Producer, getMainOpt().KafkaStatusURI.Topic);
    } catch (KafkaW::TopicCreationError const &e) {
      LOG(Sev::Error, "Can not create Kafka status producer: {}", e.what());
    }
  }

  // Interpret commands given directly in the configuration file, useful
  // for testing.
  for (auto const &Command : getMainOpt().CommandsFromJson) {
    this->handle_command(Command);
  }

  Listener.start();
  using Clock = std::chrono::steady_clock;
  auto t_last_statistics = Clock::now();
  while (Running) {
    LOG(Sev::Debug, "Master poll");

    std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> KafkaMessage =
        Listener.poll();
    if (KafkaMessage->first == KafkaW::PollStatus::Msg) {
      LOG(Sev::Debug, "Handle a command");
      this->handle_command_message(
          std::make_unique<FileWriter::Msg>(std::move(KafkaMessage->second)));
    }
    if (getMainOpt().ReportStatus &&
        Clock::now() - t_last_statistics >
            std::chrono::milliseconds(getMainOpt().StatusMasterIntervalMS)) {
      t_last_statistics = Clock::now();
      statistics();
    }

    // Remove any job which is in 'is_removable' state
    StreamMasters.erase(
        std::remove_if(StreamMasters.begin(), StreamMasters.end(),
                       [](std::unique_ptr<StreamMaster<Streamer>> &Iter) {
                         return Iter->status() ==
                                Status::StreamMasterError::IS_REMOVABLE;
                       }),
        StreamMasters.end());
  }
  LOG(Sev::Info, "calling stop on all stream_masters");
  stopStreamMasters();
  LOG(Sev::Info, "called stop on all stream_masters");
}

void Master::stopStreamMasters() {
  for (auto &StreamMaster : StreamMasters) {
    StreamMaster->stop();
  }
}

void Master::statistics() {
  if (!StatusProducer) {
    return;
  }
  using nlohmann::json;
  auto Status = json::object();
  Status["type"] = "filewriter_status_master";
  Status["service_id"] = getMainOpt().ServiceID;
  Status["files"] = json::object();
  for (auto &StreamMaster : StreamMasters) {
    auto FilewriterTaskID =
        fmt::format("{}", StreamMaster->getFileWriterTask().jobID());
    auto FilewriterTaskStatus = StreamMaster->getFileWriterTask().stats();
    Status["files"][FilewriterTaskID] = FilewriterTaskStatus;
  }
  auto Buffer = Status.dump();
  StatusProducer->produce((unsigned char *)Buffer.data(), Buffer.size());
}

void Master::stop() { Running = false; }

std::string Master::getFileWriterProcessId() const {
  return FileWriterProcessId;
}

MainOpt &Master::getMainOpt() { return MainConfig; }

std::shared_ptr<KafkaW::ProducerTopic> Master::getStatusProducer() {
  return StatusProducer;
}

} // namespace FileWriter
