// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Master.h"
#include "CommandParser.h"
#include "Errors.h"
#include "JobCreator.h"
#include "Msg.h"
#include "helper.h"
#include "json.h"
#include "logger.h"
#include <chrono>
#include <functional>
#include <unistd.h>

namespace FileWriter {

Master::Master(MainOpt &Config, std::unique_ptr<IJobCreator> Creator)
    : Logger(getLogger()), Listener(Config), MainConfig(Config),
      Creator_(std::move(Creator)) {
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

void Master::handle_command(std::unique_ptr<Msg> CommandMessage) {
  std::string Message = {CommandMessage->data(), CommandMessage->size()};

  // If Kafka message does not contain a timestamp then use current time.
  auto TimeStamp = getCurrentTimeStampMS();

  if (CommandMessage->MetaData.TimestampType !=
      RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
    TimeStamp = CommandMessage->MetaData.Timestamp;
  } else {
    Logger->info(
        "Kafka command doesn't contain timestamp, so using current time.");
  }

  handle_command(Message, TimeStamp);
}

nlohmann::json Master::parseCommand(std::string const &Command) {
  try {
    return nlohmann::json::parse(Command);
  } catch (nlohmann::json::parse_error const &Error) {
    throw std::runtime_error("Could not parse command JSON");
  }
}

void Master::handle_command(std::string const &Command,
                            std::chrono::milliseconds TimeStamp) {
  try {
    auto CommandJson = parseCommand(Command);
    auto CommandName = CommandParser::extractCommandName(CommandJson);

    if (IsWriting) {
      if (CommandName == CommandParser::StopCommand) {
        auto StopInfo = CommandParser::extractStopInformation(CommandJson);
        if (StopInfo.StopTime.count() > 0) {
          Logger->info(
              "Received request to gracefully stop file with id : {} at {} ms",
              StopInfo.JobID, StopInfo.StopTime.count());
          CurrentStreamMaster->setStopTime(StopInfo.StopTime);
        } else {
          Logger->info("Received request to gracefully stop file with id : {}",
                       StopInfo.JobID);
          CurrentStreamMaster.reset(nullptr);
          // TODO: Wait for it to finish writing?
          IsWriting = false;
        }
      } else {
        throw std::runtime_error(fmt::format(
            "The command \"{}\" is not allowed when writing.", CommandName));
      }
    } else {
      if (CommandName == CommandParser::StartCommand) {
        auto StartInfo =
            CommandParser::extractStartInformation(CommandJson, TimeStamp);
        CurrentStreamMaster = Creator_->createFileWritingJob(
            StartInfo, StatusProducer, getMainOpt(), Logger);
        IsWriting = true;
      } else {
        throw std::runtime_error(fmt::format(
            "The command \"{}\" is not allowed when idle.", CommandName));
      }
    }
  } catch (std::runtime_error const &Error) {
    Logger->error("{}", Error.what());
    logEvent(StatusProducer, StatusCode::Fail, getMainOpt().ServiceID, "N/A",
             Error.what());
    return;
  }
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

  initialiseStatusProducer();

  // Interpret commands given directly from the configuration file, if present.
  for (auto const &cmd : getMainOpt().CommandsFromJson) {
    this->handle_command(cmd, getCurrentTimeStampMS());
  }

  Listener.start();
  using Clock = std::chrono::steady_clock;
  auto t_last_statistics = Clock::now();
  while (Running) {
    std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> KafkaMessage =
        Listener.poll();
    if (KafkaMessage->first == KafkaW::PollStatus::Message) {
      Logger->debug("Command received");
      this->handle_command(
          std::make_unique<FileWriter::Msg>(std::move(KafkaMessage->second)));
    }
    if (getMainOpt().ReportStatus &&
        Clock::now() - t_last_statistics >
            getMainOpt().StatusMasterIntervalMS) {
      t_last_statistics = Clock::now();
      publishStatus();
    }
    if (CurrentStreamMaster != nullptr and
        CurrentStreamMaster->isDoneWriting()) {
      CurrentStreamMaster.reset(nullptr);
      IsWriting = false;
    }
  }
}

void Master::initialiseStatusProducer() {
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
}

void Master::publishStatus() {
  if (StatusProducer == nullptr) {
    return;
  }
  auto Status = nlohmann::json::object();
  Status["type"] = "filewriter_status_master";
  Status["service_id"] = getMainOpt().ServiceID;
  Status["files"] = nlohmann::json::object();

  if (CurrentStreamMaster != nullptr) {
    auto FilewriterTaskID = fmt::format("{}", CurrentStreamMaster->getJobId());
    auto FilewriterTaskStatus = CurrentStreamMaster->getStatus();
    Status["files"][FilewriterTaskID] = FilewriterTaskStatus;
  }

  StatusProducer->produce(Status.dump());
}

void Master::stop() { Running = false; }

std::string Master::getFileWriterProcessId() const {
  return FileWriterProcessId;
}

MainOpt &Master::getMainOpt() { return MainConfig; }

} // namespace FileWriter
