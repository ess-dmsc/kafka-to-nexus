// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Handler.h"
#include "FeedbackProducer.h"
#include "Parser.h"
#include <iostream>
#include <uuid.h>

#include <utility>

namespace Command {
using LogLevel = LogSeverity;

std::unique_ptr<Handler> Handler::create(std::string const &service_id,
                                         Kafka::BrokerSettings const &settings,
                                         std::string const &job_pool_topic,
                                         std::string const &command_topic) {
  auto pool_listener = JobListener::create(job_pool_topic, settings);
  auto command_listener = CommandListener::create(command_topic, settings);
  std::unique_ptr<FeedbackProducer> command_response =
      FeedbackProducer::create(service_id, command_topic, settings);
  return std::make_unique<Handler>(
      service_id, settings, command_topic, std::move(pool_listener),
      std::move(command_listener), std::move(command_response));
}

Handler::Handler(std::string service_id, Kafka::BrokerSettings settings,
                 std::string command_topic,
                 std::unique_ptr<JobListener> pool_listener,
                 std::unique_ptr<CommandListener> command_listener,
                 std::unique_ptr<FeedbackProducer> command_response)
    : ServiceId(std::move(service_id)), JobPool(std::move(pool_listener)),
      CommandSource(std::move(command_listener)),
      CommandResponse(std::move(command_response)),
      CommandTopicAddress(std::move(command_topic)),
      KafkaSettings(std::move(settings)) {}

void Handler::loopFunction() {
  if (!IsWritingNow()) {
    auto [poll_status, message] = JobPool->pollForJob();
    if (poll_status == Kafka::PollStatus::Message &&
        Parser::isStartCommand(message)) {
      handleStartCommand(std::move(message));
      JobPool->disconnectFromPool();
    }
  } else {
    auto [poll_status, message] = CommandSource->pollForCommand();
    if (poll_status == Kafka::PollStatus::Message &&
        Parser::isStopCommand(message)) {
      handleStopCommand(std::move(message));
    }
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

void Handler::registerIsWritingFunction(IsWritingFuncType IsWritingFunction) {
  IsWritingNow = IsWritingFunction;
}

void Handler::registerGetJobIdFunction(GetJobIdFuncType GetJobIdFunction) {
  GetJobId = GetJobIdFunction;
}

void Handler::revertCommandTopic() {
  if (UsingAltTopic) {
    Logger::Info("Reverting to default command topic: {}", CommandTopicAddress);
    CommandSource->change_topic(CommandTopicAddress);
    std::swap(AltCommandResponse, CommandResponse);
    UsingAltTopic = false;
  }
}

void Handler::switchCommandTopic(std::string const &ControlTopic,
                                 time_point const StartTime) {
  if (!ControlTopic.empty() && ControlTopic != CommandTopicAddress) {
    Logger::Info(
        R"(Connecting to an alternative command topic "{}" with starting offset "{}")",
        ControlTopic, StartTime);
    CommandSource->change_topic(ControlTopic, StartTime);
    AltCommandResponse =
        FeedbackProducer::create(ServiceId, ControlTopic, KafkaSettings);
    std::swap(CommandResponse, AltCommandResponse);
    UsingAltTopic = true;
  }
}

void Handler::sendHasStoppedMessage(std::filesystem::path const &FilePath,
                                    std::string const &Metadata) {
  Logger::Debug("Sending FinishedWriting message (Result={} JobId={} File={})",
                "Success", GetJobId(), FilePath.string());
  CommandResponse->publishStoppedMsg(ActionResult::Success, GetJobId(), "",
                                     FilePath, Metadata);
  revertCommandTopic();
}

void Handler::sendErrorEncounteredMessage(std::string const &FileName,
                                          std::string const &Metadata,
                                          std::string const &ErrorMessage) {
  Logger::Debug(
      "Sending FinishedWriting message (Result={} JobId={} File={}): {}",
      "Failure", GetJobId(), FileName, ErrorMessage);
  CommandResponse->publishStoppedMsg(ActionResult::Failure, GetJobId(),
                                     ErrorMessage, FileName, Metadata);
  revertCommandTopic();
}

bool extractStartMessage(FileWriter::Msg const &CommandMsg, StartMessage &Msg,
                         std::string &ErrorStr) {
  try {
    Msg = Parser::extractStartMessage(CommandMsg);
    return true;
  } catch (std::runtime_error &E) {
    ErrorStr = E.what();
    return false;
  }
}

bool isValidUUID(std::string const &UUIDStr) {
  try {
    auto const Id = uuids::uuid::from_string(UUIDStr);
    return !Id->is_nil() && Id->version() != uuids::uuid_version::none &&
           Id->variant() == uuids::uuid_variant::rfc;
  } catch (std::system_error const &) {
    return false;
  }
}

/// Warn if the Kafka message time is not from the immediate past.
///
/// It does not necessarily mean something is wrong,
/// but it indicates that a command was queued for some time.
///
/// \param MsgTime
void warnIfMessageIsOld(time_point MsgTime) {
  if (system_clock::now() > MsgTime + 120s) {
    Logger::Info(fmt::format("Start command's message timestamp is not very "
                             "recent, the command was queued for some time"
                             "(command created at: {}, "
                             "current time: {}).",
                             toUTCDateTime(MsgTime),
                             toUTCDateTime(system_clock::now())));
  }
}

void Handler::handleStartCommand(FileWriter::Msg CommandMsg) {
  try {
    StartMessage StartJob;

    ActionResult SendResult{ActionResult::Success};
    CmdResponse ValidationResponse = startWritingProcess(CommandMsg, StartJob);
    if (ValidationResponse.StatusCode >= 400) {
      SendResult = ActionResult::Failure;
    }

    warnIfMessageIsOld(CommandMsg.getMetaData().timestamp());
    Logger::Log(ValidationResponse.LogLevel, ValidationResponse.Message);
    if (ValidationResponse.SendResponse) {
      CommandResponse->publishResponse(
          ActionResponse::StartJob, SendResult, StartJob.JobID, StartJob.JobID,
          StartJob.StopTime, ValidationResponse.StatusCode,
          ValidationResponse.Message);
    }
  } catch (std::exception &E) {
    Logger::Critical("Unable to process start command, error was: {}",
                     E.what());
  }
}

CmdResponse Handler::startWritingProcess(const FileWriter::Msg &CommandMsg,
                                         StartMessage &StartJob) {
  if (std::string exception_message;
      !extractStartMessage(CommandMsg, StartJob, exception_message)) {
    return {LogLevel::Warn, 400, false,
            fmt::format("Failed to extract start command from flatbuffer. The "
                        "error was: {}",
                        exception_message)};
  }

  if (!StartJob.ServiceID.empty() && StartJob.ServiceID != ServiceId) {
    return {
        LogLevel::Debug, 400, false,
        fmt::format(
            R"(Rejected start command as the service id was wrong. It should be "{}", it was "{}".)",
            ServiceId, StartJob.ServiceID)};
  }

  /// \note This test should never return false as consumption of new jobs
  /// should only be possible when the current one is finished. However, there
  /// is an indication that in some cases jobs will be consumed regardless.
  /// This statement is made 2022-03-21
  if (IsWritingNow()) {
    return {LogLevel::Error, 400, true,
            fmt::format("Rejected start command as there is "
                        "currently a write job in progress.")};
  }

  if (!isValidUUID(StartJob.JobID)) {
    return {
        LogLevel::Warn, 400, true,
        fmt::format(
            R"(Rejected start command as the job id was invalid (it was: "{}").)",
            StartJob.JobID)};
  }

  if (StartJob.ControlTopic.empty()) {
    return {LogLevel::Warn, 400, true,
            fmt::format(R"(Rejected start job as control topic was empty.)")};
  }

  // Start job
  try {
    switchCommandTopic(StartJob.ControlTopic, StartJob.StartTime);
    DoStart(StartJob);
  } catch (std::exception const &E) {
    revertCommandTopic();

    return {LogLevel::Error, 500, true,
            fmt::format(
                "Failed to start filewriting job. The failure message was: {}",
                E.what())};
  }

  // Success
  return {LogLevel::Info, 201, true,
          fmt::format("Started write job with start time {} and stop time {}.",
                      toUTCDateTime(StartJob.StartTime),
                      toUTCDateTime(StartJob.StopTime))};
}

bool extractStopMessage(FileWriter::Msg const &CommandMsg, StopMessage &Msg,
                        std::string &ErrorStr) {
  try {
    Msg = Parser::extractStopMessage(CommandMsg);
    return true;
  } catch (std::runtime_error &E) {
    ErrorStr = E.what();
    return false;
  }
}

void Handler::handleStopCommand(FileWriter::Msg CommandMsg) {
  try {
    StopMessage StopJob;

    ActionResult SendResult{ActionResult::Success};
    CmdResponse ValidationResponse = stopWritingProcess(CommandMsg, StopJob);
    if (ValidationResponse.StatusCode >= 400) {
      SendResult = ActionResult::Failure;
    }

    Logger::Log(ValidationResponse.LogLevel, ValidationResponse.Message);
    if (ValidationResponse.SendResponse) {
      CommandResponse->publishResponse(
          ActionResponse::SetStopTime, SendResult, StopJob.JobID,
          StopJob.CommandID, StopJob.StopTime, ValidationResponse.StatusCode,
          ValidationResponse.Message);
    }
  } catch (std::exception &E) {
    Logger::Critical("Unable to process stop command, error was: {}", E.what());
  }
}

CmdResponse Handler::stopWritingProcess(const FileWriter::Msg &CommandMsg,
                                        StopMessage &StopJob) {
  if (std::string ExceptionMessage;
      !extractStopMessage(CommandMsg, StopJob, ExceptionMessage)) {
    return {LogLevel::Warn, 400, false,
            fmt::format("Failed to extract stop command from flatbuffer. The "
                        "error was: {}",
                        ExceptionMessage)};
  }

  std::string ResponseMessage;

  if (!(StopJob.ServiceID.empty()) && ServiceId != StopJob.ServiceID) {
    return {
        LogLevel::Debug, 0, false,
        fmt::format(
            "Rejected stop command as the service ID did not match. Local ID "
            "is {}, command ID was {}.",
            ServiceId, StopJob.ServiceID)};
  }

  if (!IsWritingNow()) {
    return {LogLevel::Warn, 400,
            !StopJob.ServiceID.empty() && ServiceId == StopJob.ServiceID,
            fmt::format("Rejected stop command as there is "
                        "currently no write job in progress.")};
  }

  auto CurrentJobId = GetJobId();
  if (CurrentJobId != StopJob.JobID) {
    return {
        LogLevel::Warn, 400,
        !StopJob.ServiceID.empty() && ServiceId == StopJob.ServiceID,
        fmt::format("Rejected stop command as the job ID did not match (local"
                    "ID is {}, command ID was: {}).",
                    CurrentJobId, StopJob.JobID)};
  }

  if (!isValidUUID(StopJob.CommandID)) {
    return {LogLevel::Error, 400, true,
            fmt::format("Rejected stop command as the command ID was invalid "
                        "(it was: {}).",
                        StopJob.CommandID)};
  }

  // Stop job
  try {
    if (StopJob.StopTime == time_point{0ms}) {
      DoStopNow();
      ResponseMessage = "Attempting to stop writing job now.";
    } else {
      DoSetStopTime(StopJob.StopTime);
      ResponseMessage =
          fmt::format("File writing job stop time set to: {}",
                      toUTCDateTime(time_point(StopJob.StopTime)));
    }
  } catch (std::exception const &E) {
    ResponseMessage = E.what();
    return {LogLevel::Error, 500, true,
            fmt::format("Failed to execute stop command. The "
                        "failure message was: {}",
                        ResponseMessage)};
  }

  // Success
  return {LogLevel::Info, 201, true, ResponseMessage};
}

} // namespace Command
