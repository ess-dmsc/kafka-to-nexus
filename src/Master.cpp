// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Master.h"
#include "CommandHandler.h"
#include "CommandParser.h"
#include "Errors.h"
#include "Msg.h"
#include "json.h"
#include "logger.h"
#include "helper.h"
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
  } catch (nlohmann::json::parse_error const & Error) {
    throw std::runtime_error("Could not parse command JSON");
  }
}

void Master::handle_command(std::string const &Command, std::chrono::milliseconds TimeStamp) {
  try {
    auto CommandJson =  parseCommand(Command);
    auto CommandName = CommandParser::extractCommandName(CommandJson);

    if (CommandName == CommandParser::StartCommand) {
      auto StartInfo =
          CommandParser::extractStartInformation(CommandJson, TimeStamp);

      // Check job is not already running
      if (StreamsControl->jobIDInUse(StartInfo.JobID)) {
        throw std::runtime_error(fmt::format("Command ignored as job id {} is already in progress",
                                             StartInfo.JobID));
      }

      CommandHandler Handler;
      auto NewJob = Handler.createFileWritingJob(StartInfo, StatusProducer, getMainOpt());
      StreamsControl->addStreamMaster(std::move(NewJob));
    }
    else if (CommandName == CommandParser::StopCommand) {
      auto StopInfo = CommandParser::extractStopInformation(CommandJson);
      if (StopInfo.StopTime.count() > 0) {
        Logger->info(
            "Received request to gracefully stop file with id : {} at {} ms",
            StopInfo.JobID, StopInfo.StopTime.count());
        StreamsControl->setStopTimeForJob(StopInfo.JobID, StopInfo.StopTime);
      } else {
        Logger->info("Received request to gracefully stop file with id : {}",
                     StopInfo.JobID);
        StreamsControl->stopJob(StopInfo.JobID);
      }
    } else if (CommandName == CommandParser::StopAllWritingCommand) {
      StreamsControl->stopStreamMasters();
    } else if (CommandName == CommandParser::ExitCommand) {
     stop();
    } else {
      throw std::runtime_error(fmt::format("Did not recognise command name {}", CommandName));
    }

  }
  catch(std::runtime_error const & Error) {
    Logger->error("{}", Error.what());
//    logEvent(StatusProducer, StatusCode::Fail, Config.ServiceID, JobID,
//             Message);
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
            std::chrono::milliseconds(getMainOpt().StatusMasterIntervalMS)) {
      t_last_statistics = Clock::now();
      statistics();
    }

    StreamsControl->deleteRemovable();
  }
  Logger->info("calling stop on all stream_masters");
  StreamsControl->stopStreamMasters();
  Logger->info("called stop on all stream_masters");
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
  // TODO
//  for (auto &StreamMaster : StreamMasters) {
//    auto FilewriterTaskID =
//        fmt::format("{}", StreamMaster->getFileWriterTask().jobID());
//    auto FilewriterTaskStatus = StreamMaster->getFileWriterTask().stats();
//    Status["files"][FilewriterTaskID] = FilewriterTaskStatus;
//  }
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
