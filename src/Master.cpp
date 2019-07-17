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

Master::Master(MainOpt &Config)
    : Logger(getLogger()), Listener(Config), MainConfig(Config) {
  std::vector<char> buffer;
  buffer.resize(128);
  gethostname(buffer.data(), buffer.size());
  if (buffer.back() != 0) {
    // likely an error
    buffer.back() = 0;
    Logger->info("Hostname got truncated: {}", buffer.data());
  }
  std::string hostname(buffer.data());
  FileWriterProcessId =
      fmt::format("kafka-to-nexus--{}--{}", hostname, getpid_wrapper());
  Logger->info("getFileWriterProcessId: {}", Master::getFileWriterProcessId());
}

void Master::handle_command_message(std::unique_ptr<Msg> CommandMessage) {
  CommandHandler command_handler(getMainOpt(), this);
  if (CommandMessage->MetaData.TimestampType !=
      RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
    auto TempTimeStamp = CommandMessage->MetaData.Timestamp;
    command_handler.tryToHandle(std::move(CommandMessage), TempTimeStamp);
  } else {
    command_handler.tryToHandle(std::move(CommandMessage));
  }
}

void Master::handle_command(std::string const &Command) {
  CommandHandler command_handler(getMainOpt(), this);
  command_handler.tryToHandle(Command);
}

std::unique_ptr<StreamMaster> &
Master::getStreamMasterForJobID(std::string const &JobID) {
  for (auto &StreamMaster : StreamMasters) {
    if (StreamMaster->getJobId() == JobID) {
      return StreamMaster;
    }
  }
  static std::unique_ptr<StreamMaster> NotFound;
  return NotFound;
}

void Master::addStreamMaster(std::unique_ptr<StreamMaster> StreamMaster) {
  StreamMasters.push_back(std::move(StreamMaster));
}

struct OnScopeExit {
  explicit OnScopeExit(std::function<void()> Action)
      : ExitAction(std::move(Action)), Logger(getLogger()){};
  ~OnScopeExit() {
    try {
      ExitAction();
    } catch (std::bad_function_call &Error) {
      Logger->warn("OnScopeExit::~OnScopeExit(): Failure to call.");
    }
  };
  std::function<void()> ExitAction;
  SharedLogger Logger;
};

void Master::run() {
  OnScopeExit SetExitFlag([this]() { HasExitedRunLoop = true; });
  // Set up connection to the Kafka status topic if desired.
  if (getMainOpt().ReportStatus) {
    Logger->info("Publishing status to kafka://{}/{}",
                 getMainOpt().KafkaStatusURI.HostPort,
                 getMainOpt().KafkaStatusURI.Topic);
    KafkaW::BrokerSettings BrokerSettings;
    BrokerSettings.Address = getMainOpt().KafkaStatusURI.HostPort;
    auto producer = std::make_shared<KafkaW::Producer>(BrokerSettings);
    try {
      StatusProducer = std::make_shared<KafkaW::ProducerTopic>(
          producer, getMainOpt().KafkaStatusURI.Topic);
    } catch (KafkaW::TopicCreationError const &e) {
      Logger->error("Can not create Kafka status producer: {}", e.what());
    }
  }

  // Interpret commands given directly in the configuration file, useful
  // for testing.
  for (auto const &cmd : getMainOpt().CommandsFromJson) {
    this->handle_command(cmd);
  }

  Listener.start();
  using Clock = std::chrono::steady_clock;
  auto t_last_statistics = Clock::now();
  while (Running) {
    std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> KafkaMessage =
        Listener.poll();
    if (KafkaMessage->first == KafkaW::PollStatus::Message) {
      Logger->debug("Handle a command");
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
    StreamMasters.erase(std::remove_if(StreamMasters.begin(),
                                       StreamMasters.end(),
                                       [](std::unique_ptr<StreamMaster> &Iter) {
                                         return Iter->isRemovable();
                                       }),
                        StreamMasters.end());
  }
  Logger->info("calling stop on all stream_masters");
  stopStreamMasters();
  Logger->info("called stop on all stream_masters");
}

void Master::stopStreamMasters() {
  for (auto &x : StreamMasters) {
    x->requestStop();
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
  std::string Buffer = Status.dump();
  StatusProducer->produce(Buffer);
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
