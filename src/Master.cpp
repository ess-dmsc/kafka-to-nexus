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
        }
        throw std::runtime_error("Start command is not allowed when writing");
      } else {
        if (CommandParser::isStartCommand(Command)) {
          auto const StartInfo =
              CommandParser::extractStartInformation(Command, TimeStamp);
          return States::StartRequested{StartInfo};
        }
        throw std::runtime_error("Stop command is not allowed when idle");
      }
    }
  } catch (std::runtime_error const &Error) {
    getLogger()->error("{}", Error.what());
  }
  return CurrentState;
}

Master::Master(MainOpt &Config, std::unique_ptr<CommandListener> Listener,
               std::unique_ptr<IJobCreator> Creator,
               std::unique_ptr<Status::StatusReporter> Reporter,
               Metrics::Registrar const &Registrar)
    : Logger(getLogger()), MainConfig(Config), CmdListener(std::move(Listener)),
      Creator_(std::move(Creator)), Reporter(std::move(Reporter)),
      MasterMetricsRegistrar(Registrar) {
  CmdListener->start();
  Logger->info("getFileWriterProcessId: {}", Config.ServiceID);
}

FileWriterState Master::handleCommand(Msg const &CommandMessage) {
  // If Kafka message does not contain a timestamp then use current time.
  auto TimeStamp = getCurrentTimeStampMS();

  if (CommandMessage.getMetaData().TimestampType !=
      RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
    TimeStamp = CommandMessage.getMetaData().Timestamp;
  } else {
    Logger->info("Command doesn't contain timestamp, so using current time.");
  }

  return getNextState(CommandMessage, TimeStamp, CurrentState);
}

void Master::startWriting(StartCommandInfo const &StartInfo) {
  Logger->info("Received request to start writing file with id : {} at "
               "time {} ms",
               StartInfo.JobID, StartInfo.StartTime.count());
  try {
    CurrentState = States::Writing();
    Reporter->updateStatusInfo({StartInfo.JobID, StartInfo.Filename,
                                StartInfo.StartTime, StartInfo.StopTime});
    CurrentStreamController = Creator_->createFileWritingJob(
        StartInfo, MainConfig, Logger, MasterMetricsRegistrar);
  } catch (std::runtime_error const &Error) {
    Logger->error("{}", Error.what());
    setToIdle();
  }
}

void Master::requestStopWriting(StopCommandInfo const &StopInfo) {
  if (StopInfo.JobID != CurrentStreamController->getJobId()) {
    Logger->info(
        "Stop request's job id ({}) does not match running job's id ({}), "
        "so ignoring",
        StopInfo.JobID, CurrentStreamController->getJobId());
    return;
  }

  Logger->info("Received request to stop file with id : {} at time {} ms",
               StopInfo.JobID, StopInfo.StopTime.count());
  CurrentStreamController->setStopTime(StopInfo.StopTime);
  Reporter->updateStopTime(StopInfo.StopTime);
}

bool Master::hasWritingStopped() {
  return CurrentStreamController != nullptr and
         CurrentStreamController->isDoneWriting();
}

void Master::moveToNewState(FileWriterState const &NewState) {
  if (auto StartReq = mpark::get_if<States::StartRequested>(&NewState)) {
    startWriting(StartReq->StartInfo);
  } else if (auto StopReq = mpark::get_if<States::StopRequested>(&NewState)) {
    requestStopWriting(StopReq->StopInfo);
  }
}

void Master::run() {
  auto const KafkaMessage = CmdListener->poll();
  if (KafkaMessage.first == Kafka::PollStatus::Message) {
    Logger->debug("Command received");
    moveToNewState(this->handleCommand(KafkaMessage.second));
  }

  // Doesn't stop immediately when commanded to.
  // Also, can stop even if not commanded to.
  if (hasWritingStopped()) {
    setToIdle();
  }
}

bool Master::isWriting() const {
  return mpark::get_if<States::Idle>(&CurrentState) == nullptr;
}

void Master::setToIdle() {
  CurrentStreamController.reset(nullptr);
  CurrentState = States::Idle();
  Reporter->resetStatusInfo();
}

} // namespace FileWriter
