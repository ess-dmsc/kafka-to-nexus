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
using LogLevel = Log::Severity;

std::unique_ptr<Handler> Handler::create(std::string const &service_id,
                                         Kafka::BrokerSettings const &settings,
                                         std::string const &job_pool_topic,
                                         std::string const &command_topic) {
  auto pool_listener = std::make_unique<JobListener>(job_pool_topic, settings);
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
      handleStartCommand(std::move(message), true);
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
    LOG_INFO("Reverting to default command topic: {}", CommandTopicAddress);
    CommandSource->change_topic(CommandTopicAddress);
    std::swap(AltCommandResponse, CommandResponse);
    UsingAltTopic = false;
  }
}

void Handler::switchCommandTopic(std::string const &ControlTopic,
                                 time_point const StartTime) {
  LOG_INFO(
      R"(Connecting to an alternative command topic "{}" with starting offset "{}")",
      ControlTopic, StartTime);
  CommandSource->change_topic(ControlTopic, StartTime);
  AltCommandResponse =
      FeedbackProducer::create(ServiceId, ControlTopic, KafkaSettings);
  std::swap(CommandResponse, AltCommandResponse);
  UsingAltTopic = true;
}

void Handler::sendHasStoppedMessage(std::filesystem::path const &FilePath,
                                    std::string const &Metadata) {
  LOG_DEBUG("Sending FinishedWriting message (Result={} JobId={} File={})",
            "Success", GetJobId(), FilePath.string());
  CommandResponse->publishStoppedMsg(ActionResult::Success, GetJobId(), "",
                                     FilePath, Metadata);
  revertCommandTopic();
}

void Handler::sendErrorEncounteredMessage(std::string const &FileName,
                                          std::string const &Metadata,
                                          std::string const &ErrorMessage) {
  LOG_DEBUG("Sending FinishedWriting message (Result={} JobId={} File={}): {}",
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
    LOG_WARN(fmt::format("Start command's message timestamp is not very "
                         "recent, the command was queued for some time"
                         "(command created at: {}, "
                         "current time: {}).",
                         toUTCDateTime(MsgTime),
                         toUTCDateTime(system_clock::now())));
  }
}

void Handler::handleStartCommand(FileWriter::Msg CommandMsg,
                                 bool IsJobPoolCommand) {
  try {
    StartMessage StartJob;

    ActionResult SendResult{ActionResult::Success};
    CmdResponse ValidationResponse =
        startWritingProcess(CommandMsg, StartJob, IsJobPoolCommand);
    if (ValidationResponse.StatusCode >= 400) {
      SendResult = ActionResult::Failure;
    }

    warnIfMessageIsOld(CommandMsg.getMetaData().timestamp());
    Log::Msg(ValidationResponse.LogLevel, ValidationResponse.MessageString());
    if (ValidationResponse.SendResponse) {
      CommandResponse->publishResponse(
          ActionResponse::StartJob, SendResult, StartJob.JobID, StartJob.JobID,
          StartJob.StopTime, ValidationResponse.StatusCode,
          ValidationResponse.MessageString());
    }
    if (SendResult == ActionResult::Failure) {
      revertCommandTopic();
    }
  } catch (std::exception &E) {
    LOG_CRITICAL("Unable to process start command, error was: {}", E.what());
  }
}

CmdResponse Handler::startWritingProcess(const FileWriter::Msg &CommandMsg,
                                         StartMessage &StartJob,
                                         bool IsJobPoolCommand) {
  std::string ExceptionMessage;
  if (!extractStartMessage(CommandMsg, StartJob, ExceptionMessage)) {
    return CmdResponse{
        LogLevel::Warning, 400, false, [ExceptionMessage]() {
          return fmt::format(
              "Failed to extract start command from flatbuffer. The "
              "error was: {}",
              ExceptionMessage);
        }};
  }
  return startWriting(StartJob, IsJobPoolCommand);
}

CmdResponse Handler::startWriting(StartMessage const &StartJob,
                                  bool IsJobPoolCommand) {
  std::string ExceptionMessage;

  if (IsJobPoolCommand && !StartJob.ServiceID.empty() &&
      StartJob.ServiceID != ServiceId) {
    return CmdResponse{
        LogLevel::Debug, 400, false, [StartJob, this]() {
          return fmt::format(
              R"(Rejected start command as the service id was wrong. It should be "{}", it was "{}".)",
              ServiceId, StartJob.ServiceID);
        }};
  }

  /// \note This test should never return false as consumption of new jobs
  /// should only be possible when the current one is finished. However, there
  /// is an indication that in some cases jobs will be consumed regardless.
  /// This statement is made 2022-03-21
  if (IsWritingNow()) {
    return CmdResponse{LogLevel::Error, 400, true, []() {
                         return fmt::format(
                             "Rejected start command as there is "
                             "currently a write job in progress.");
                       }};
  }

  if (StartJob.ControlTopic.empty()) {
    return CmdResponse {
      LogLevel::Warning, 400, true, [StartJob]() {
        return fmt::format(
          R"(Rejected start job as control topic was empty.)");
    }};
  }

  if (!StartJob.ControlTopic.empty()) {
    if (!IsJobPoolCommand) {
      return CmdResponse{
          LogLevel::Error, 400, true, [StartJob]() {
            return fmt::format(
                R"(Rejected new/alternative command topic ("{}") as the job was not received from job pool.)",
                StartJob.ControlTopic);
          }};
    }
    switchCommandTopic(StartJob.ControlTopic, StartJob.StartTime);
  }

  if (!isValidUUID(StartJob.JobID)) {
    return CmdResponse{
        LogLevel::Warning, 400, true, [StartJob]() {
          return fmt::format(
              R"(Rejected start command as the job id was invalid (it was: "{}").)",
              StartJob.JobID);
        }};
  }

  // Start job
  try {
    DoStart(StartJob);
  } catch (std::exception const &E) {
    ExceptionMessage = E.what();

    return CmdResponse{
        LogLevel::Error, 500, true, [ExceptionMessage]() {
          return fmt::format(
              "Failed to start filewriting job. The failure message was: {}",
              ExceptionMessage);
        }};
  }

  // Success
  return CmdResponse{
      LogLevel::Info, 201, true, [StartJob]() {
        return fmt::format(
            "Started write job with start time {} and stop time {}.",
            toUTCDateTime(StartJob.StartTime),
            toUTCDateTime(StartJob.StopTime));
      }};
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

    Log::Msg(ValidationResponse.LogLevel, ValidationResponse.MessageString());
    if (ValidationResponse.SendResponse) {
      CommandResponse->publishResponse(
          ActionResponse::SetStopTime, SendResult, StopJob.JobID,
          StopJob.CommandID, StopJob.StopTime, ValidationResponse.StatusCode,
          ValidationResponse.MessageString());
    }
  } catch (std::exception &E) {
    LOG_CRITICAL("Unable to process stop command, error was: {}", E.what());
  }
}

CmdResponse Handler::stopWritingProcess(const FileWriter::Msg &CommandMsg,
                                        StopMessage &StopJob) {
  if (std::string ExceptionMessage;
      !extractStopMessage(CommandMsg, StopJob, ExceptionMessage)) {
    return CmdResponse{
        LogLevel::Warning, 400, false, [ExceptionMessage]() {
          return fmt::format(
              "Failed to extract stop command from flatbuffer. The "
              "error was: {}",
              ExceptionMessage);
        }};
  }
  return stopWriting(StopJob);
}

CmdResponse Handler::stopWriting(StopMessage const &StopCmd) {
  std::string ResponseMessage;

  if (!(StopCmd.ServiceID.empty()) && ServiceId != StopCmd.ServiceID) {
    return CmdResponse{
        LogLevel::Debug, 0, false, [StopCmd, this]() {
          return fmt::format(
              "Rejected stop command as the service ID did not match. Local ID "
              "is {}, command ID was {}.",
              ServiceId, StopCmd.ServiceID);
        }};
  }

  if (!IsWritingNow()) {
    return CmdResponse{
        LogLevel::Warning, 400,
        !StopCmd.ServiceID.empty() && ServiceId == StopCmd.ServiceID, []() {
          return fmt::format("Rejected stop command as there is "
                             "currently no write job in progress.");
        }};
  }

  auto CurrentJobId = GetJobId();
  if (!(CurrentJobId == StopCmd.JobID)) {
    return CmdResponse{
        LogLevel::Warning, 400,
        !StopCmd.ServiceID.empty() && ServiceId == StopCmd.ServiceID,
        [CurrentJobId, StopCmd]() {
          return fmt::format(
              "Rejected stop command as the job ID did not match (local"
              "ID is {}, command ID was: {}).",
              CurrentJobId, StopCmd.JobID);
        }};
  }

  if (!isValidUUID(StopCmd.CommandID)) {
    return CmdResponse{
        LogLevel::Error, 400, true, [StopCmd]() {
          return fmt::format(
              "Rejected stop command as the command ID was invalid "
              "(it was: {}).",
              StopCmd.CommandID);
        }};
  }

  // Stop job
  try {
    if (StopCmd.StopTime == time_point{0ms}) {
      DoStopNow();
      ResponseMessage = "Attempting to stop writing job now.";
    } else {
      DoSetStopTime(StopCmd.StopTime);
      ResponseMessage =
          fmt::format("File writing job stop time set to: {}",
                      toUTCDateTime(time_point(StopCmd.StopTime)));
    }
  } catch (std::exception const &E) {
    ResponseMessage = E.what();
    return CmdResponse{LogLevel::Error, 500, true, [ResponseMessage]() {
                         return fmt::format(
                             "Failed to execute stop command. The "
                             "failure message was: {}",
                             ResponseMessage);
                       }};
  }

  // Success
  return CmdResponse{LogLevel::Info, 201, true,
                     [ResponseMessage]() { return ResponseMessage; }};
}

} // namespace Command
