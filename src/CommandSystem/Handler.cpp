// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Handler.h"
#include "Parser.h"
#include "Msg.h"
#include "TimeUtility.h"


namespace Command {

bool isJobIdValid(std::string JobId) {
  return not JobId.empty();
}

Handler::Handler(std::string ServiceIdentifier, Kafka::BrokerSettings const &Settings, uri::URI JobPoolUri, uri::URI CommandTopicUri) :
  Handler(ServiceIdentifier, std::make_unique<JobListener>(JobPoolUri, Settings), std::make_unique<CommandListener>(CommandTopicUri, Settings), std::make_unique<ResponseProducer>(ServiceIdentifier, CommandTopicUri, Settings)) {
}

Handler::Handler(std::string ServiceIdentifier, std::unique_ptr<JobListener> JobConsumer, std::unique_ptr<CommandListener> CommandConsumer, std::unique_ptr<ResponseProducer> Response) : ServiceId(ServiceIdentifier), JobPool(std::move(JobConsumer)), CommandSource(std::move(CommandConsumer)), CommandResponse(std::move(Response)) {

}

void Handler::loopFunction() {
  if (PollForJob) {
    auto JobMsg = JobPool->pollForJob();
    if (JobMsg.first == Kafka::PollStatus::Message) {
      handleCommand(std::move(JobMsg.second), true);
    }
  }
  auto CommandMsg = CommandSource->pollForCommand();
  if (CommandMsg.first == Kafka::PollStatus::Message) {
    handleCommand(std::move(CommandMsg.second), false);
  }
}

void Handler::registerStartFunction(StartFuncType StartFunction) {
  DoStart = StartFunction;
}

void Handler::registerSetStopTimeFunction(StopTimeFuncType StopTimeFunction) {
  DoSetStopTime = StopTimeFunction;
}

void Handler::registerStopNowFunction(StopNowFuncType StopNowFunction) {
  DoStopNow = StopNowFunction;
}

void Handler::sendHasStoppedMessage() {
  CommandResponse->publishResponse(ActionResponse::HasStopped, ActionResult::Success, JobId, "");
  PollForJob = true;
}

void Handler::sendErrorEncounteredMessage(std::string ErrorMessage) {
  CommandResponse->publishResponse(ActionResponse::HasStopped, ActionResult::Failure, JobId, ErrorMessage);
}

void Handler::handleCommand(FileWriter::Msg CommandMsg, bool IgnoreServiceId) {
  if (Parser::isStartCommand(CommandMsg)) {
    handleStartCommand(std::move(CommandMsg), IgnoreServiceId);
  } else if (Parser::isStopCommand(CommandMsg)) {
    handleStopCommand(std::move(CommandMsg));
  }
}

void Handler::handleStartCommand(FileWriter::Msg CommandMsg, bool IgnoreServiceId) {
  try {
    auto StartJob = Parser::extractStartInformation(CommandMsg);
    if (not IgnoreServiceId and StartJob.ServiceID != ServiceId) {
      CommandResponse->publishResponse(
          ActionResponse::StartJob, ActionResult::Failure, StartJob.JobID,
          "Unable to start filewriting job: service id is incorrect.");
      LOG_DEBUG("Rejected start command as the service id was wrong. It should be {}, it was {}.",
                ServiceId, JobId);
      return;
    }
    if (isJobIdValid(StartJob.JobID)) {
      CommandResponse->publishResponse(
          ActionResponse::StartJob, ActionResult::Failure, StartJob.JobID,
          "Unable to start filewriting job: job id was invalid.");
      LOG_DEBUG(
          "Rejected start command as the job id was invalid (it was: {}).",
          StartJob.JobID);
      return;
    }
    try {
      DoStart(StartJob);
      JobId = StartJob.JobID;
      PollForJob = false;
      JobPool->disconnectFromPool();
    } catch (std::exception const &E) {
      LOG_ERROR("Failed to start filewriting job. The failure message was: {}",
                E.what());
      CommandResponse->publishResponse(
          ActionResponse::StartJob, ActionResult::Failure, StartJob.JobID,
          "Unable to start filewriting job. Error message was: " +
              std::string(E.what()));
      PollForJob = true;
      JobId = "";
      return;
    }
    LOG_INFO("Starting write job.");
    CommandResponse->publishResponse(ActionResponse::StartJob,
                                     ActionResult::Success, StartJob.JobID, "");
  } catch (std::runtime_error &E) {
    LOG_ERROR("Unable to process start command, error was: {}", E.what());
  }
}

void Handler::handleStopCommand(FileWriter::Msg CommandMsg) {
  try {
    auto StopJob = Parser::extractStopInformation(CommandMsg);
    if (ServiceId != StopJob.ServiceID or JobId != StopJob.JobID) {
      LOG_DEBUG("Rejected stop command as job and/or service id was incorrect (was {}/{}, should be {}/{}).",
                StopJob.JobID, StopJob.ServiceID, JobId, ServiceId);
      return;
    }
    if (StopJob.StopTime == 0ms) {
      try {
        DoStopNow();
      } catch (std::exception const &E) {
        auto ErrorStr = fmt::format(
            "Unable to stop filewriting job immediately. Error message was: {}",
            E.what());
        CommandResponse->publishResponse(
            ActionResponse::StopNow, ActionResult::Failure, JobId, ErrorStr);
        LOG_ERROR(ErrorStr);
      }
      CommandResponse->publishResponse(ActionResponse::StopNow,
                                       ActionResult::Success, JobId, "");
      LOG_INFO("Attempting to stop writing job now.");
    } else {
      try {
        DoSetStopTime(StopJob.StopTime);
      } catch (std::exception const &E) {
        auto ErrorStr = fmt::format("Unable to set stop time for filewriting job. Error message was: {}",
                                    E.what());
        CommandResponse->publishResponse(ActionResponse::SetStopTime,
                                         ActionResult::Failure, JobId,
                                         ErrorStr);
        LOG_ERROR(ErrorStr);
      }
      CommandResponse->publishResponse(ActionResponse::SetStopTime,
                                       ActionResult::Success, JobId, "");
      LOG_INFO("File writing job stop time set to: {}",
               toUTCDateTime(time_point(StopJob.StopTime)));
    }
  } catch (std::runtime_error &E) {
    LOG_ERROR("Unable to process stop command, error was: {}", E.what());
  }
}

} // namespace Command
