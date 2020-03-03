// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Master.h"
#include "CommandListener.h"
#include "CommandParser.h"
#include "JobCreator.h"
#include "Status/StatusReporter.h"
#include "helper.h"
#include "logger.h"
#include <chrono>
#include <functional>

namespace FileWriter {

FileWriterState getNextState(Msg const &Command,
                             std::chrono::milliseconds TimeStamp,
                             FileWriterState const &CurrentState) {
  try {
    if (CommandParser::isStopCommand(Command) ||
        CommandParser::isStartCommand(Command)) {
      if (mpark::get_if<States::Writing>(&CurrentState)) {
        if (CommandParser::isStopCommand(Command)) {
          auto StopInfo = CommandParser::extractStopInformation(Command);
          if (StopInfo.StopTime.count() == 0) {
            StopInfo.StopTime = getCurrentTimeStampMS();
          }
          return States::StopRequested{StopInfo};
        } else {
          throw std::runtime_error("Start command is not allowed when writing");
        }
      } else {
        if (CommandParser::isStartCommand(Command)) {
          auto const StartInfo =
              CommandParser::extractStartInformation(Command, TimeStamp);
          return States::StartRequested{StartInfo};
        } else {
          throw std::runtime_error("Stop command is not allowed when idle");
        }
      }
    }
  } catch (std::runtime_error const &Error) {
    getLogger()->error("{}", Error.what());
  }
  return CurrentState;
}

Master::Master(MainOpt &Config, std::unique_ptr<CommandListener> Listener,
               std::unique_ptr<IJobCreator> Creator,
               std::unique_ptr<Status::StatusReporter> Reporter)
    : Logger(getLogger()), MainConfig(Config), CmdListener(std::move(Listener)),
      Creator_(std::move(Creator)), Reporter(std::move(Reporter)) {
  CmdListener->start();
  Logger->info("getFileWriterProcessId: {}", Config.ServiceID);
}

FileWriterState Master::handleCommand(Msg const &CommandMessage) {
  // If Kafka message does not contain a timestamp then use current time.
  auto TimeStamp = getCurrentTimeStampMS();

  if (CommandMessage.MetaData.TimestampType !=
      RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
    TimeStamp = CommandMessage.MetaData.Timestamp;
  } else {
    Logger->info("Command doesn't contain timestamp, so using current time.");
  }

  return getNextState(CommandMessage, TimeStamp, CurrentState);
}

void Master::startWriting(StartCommandInfo const &StartInfo) {
  Logger->info("Received request to start writing file with id : {} at "
               "time {} ms",
               StartInfo.JobID, StartInfo.StartTime.count());
  CurrentStreamMaster =
      Creator_->createFileWritingJob(StartInfo, MainConfig, Logger);
  Reporter->updateStatusInfo({StartInfo.JobID, StartInfo.Filename,
                              StartInfo.StartTime, StartInfo.StopTime});
}

void Master::requestStopWriting(StopCommandInfo const &StopInfo) {
  if (StopInfo.JobID != CurrentStreamMaster->getJobId()) {
    Logger->info("Stop request's job id {} does not match running job's id {}, "
                 "so ignoring",
                 StopInfo.JobID, CurrentStreamMaster->getJobId());
    return;
  }

  Logger->info("Received request to stop file with id : {} at time {} ms",
               StopInfo.JobID, StopInfo.StopTime.count());
  CurrentStreamMaster->setStopTime(StopInfo.StopTime);
  Reporter->updateStopTime(StopInfo.StopTime);
}

bool Master::hasWritingStopped() {
  return CurrentStreamMaster != nullptr and
         CurrentStreamMaster->isDoneWriting();
}

void Master::moveToNewState(FileWriterState const &NewState) {
  if (auto StartReq = mpark::get_if<States::StartRequested>(&NewState)) {
    try {
      startWriting(StartReq->StartInfo);
      CurrentState = States::Writing();
    } catch (std::runtime_error const &Error) {
      Logger->error("{}", Error.what());
    }
  } else if (auto StopReq = mpark::get_if<States::StopRequested>(&NewState)) {
    requestStopWriting(StopReq->StopInfo);
  }
}

std::pair<KafkaW::PollStatus, Msg> Master::pollForMessage() {
  auto KafkaMessage = CmdListener->poll();
  if (KafkaMessage.first == KafkaW::PollStatus::Message) {
    return KafkaMessage;
  }
  return {KafkaW::PollStatus::Empty, Msg()};
}

void Master::run() {
  auto const KafkaMessage = pollForMessage();
  if (KafkaMessage.first == KafkaW::PollStatus::Message) {
    Logger->debug("Command received");
    moveToNewState(this->handleCommand(KafkaMessage.second));
  }

  // Doesn't stop immediately when commanded to.
  // Also, can stop even if not commanded to.
  if (hasWritingStopped()) {
    CurrentStreamMaster.reset(nullptr);
    CurrentState = States::Idle();
    Reporter->resetStatusInfo();
  }
}

bool Master::isWriting() const {
  return mpark::get_if<States::Idle>(&CurrentState) == nullptr;
}

} // namespace FileWriter
