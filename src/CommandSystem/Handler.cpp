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

void Handler::registerStartFunction(StartFuncType start_function) {
  DoStart = start_function;
}

void Handler::registerSetStopTimeFunction(
    StopTimeFuncType set_stop_time_function) {
  DoSetStopTime = set_stop_time_function;
}

void Handler::registerStopNowFunction(StopNowFuncType stop_now_function) {
  DoStopNow = stop_now_function;
}

void Handler::registerIsWritingFunction(IsWritingFuncType is_writing_function) {
  IsWritingNow = is_writing_function;
}

void Handler::registerGetJobIdFunction(GetJobIdFuncType get_job_id_function) {
  GetJobId = get_job_id_function;
}

void Handler::revertCommandTopic() {
  if (UsingAltTopic) {
    Logger::Info("Reverting to default command topic: {}", CommandTopicAddress);
    CommandSource->change_topic(CommandTopicAddress);
    std::swap(AltCommandResponse, CommandResponse);
    UsingAltTopic = false;
  }
}

void Handler::switchCommandTopic(std::string const &control_topic,
                                 time_point const start_time) {
  if (!control_topic.empty() && control_topic != CommandTopicAddress) {
    Logger::Info(
        R"(Connecting to an alternative command topic "{}" with starting offset "{}")",
        control_topic, start_time);
    CommandSource->change_topic(control_topic, start_time);
    AltCommandResponse =
        FeedbackProducer::create(ServiceId, control_topic, KafkaSettings);
    std::swap(CommandResponse, AltCommandResponse);
    UsingAltTopic = true;
  }
}

void Handler::sendHasStoppedMessage(std::filesystem::path const &file_path,
                                    std::string const &meta_data) {
  Logger::Debug("Sending FinishedWriting message (Result={} JobId={} File={})",
                "Success", GetJobId(), file_path.string());
  CommandResponse->publishStoppedMsg(ActionResult::Success, GetJobId(), "",
                                     file_path, meta_data);
  revertCommandTopic();
}

void Handler::sendErrorEncounteredMessage(std::string const &filename,
                                          std::string const &metadata,
                                          std::string const &error_message) {
  Logger::Debug(
      "Sending FinishedWriting message (Result={} JobId={} File={}): {}",
      "Failure", GetJobId(), filename, error_message);
  CommandResponse->publishStoppedMsg(ActionResult::Failure, GetJobId(),
                                     error_message, filename, metadata);
  revertCommandTopic();
}

bool extractStartMessage(FileWriter::Msg const &command_message,
                         StartMessage &message, std::string &error_string) {
  try {
    message = Parser::extractStartMessage(command_message);
    return true;
  } catch (std::runtime_error &E) {
    error_string = E.what();
    return false;
  }
}

bool isValidUUID(std::string const &uuid) {
  try {
    auto const Id = uuids::uuid::from_string(uuid);
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
void warnIfMessageIsOld(time_point message_time) {
  if (system_clock::now() > message_time + 120s) {
    Logger::Info(fmt::format("Start command's message timestamp is not very "
                             "recent, the command was queued for some time"
                             "(command created at: {}, "
                             "current time: {}).",
                             toUTCDateTime(message_time),
                             toUTCDateTime(system_clock::now())));
  }
}

void Handler::handleStartCommand(FileWriter::Msg start_message) {
  try {
    StartMessage StartJob;

    ActionResult SendResult{ActionResult::Success};
    CmdResponse ValidationResponse =
        startWritingProcess(start_message, StartJob);
    if (ValidationResponse.StatusCode >= 400) {
      SendResult = ActionResult::Failure;
    }

    warnIfMessageIsOld(start_message.getMetaData().timestamp());
    Logger::Log(ValidationResponse.LogLevel, ValidationResponse.Message);

    CommandResponse->echo_message(start_message);

    CommandResponse->publishResponse(
        ActionResponse::StartJob, SendResult, StartJob.JobID, StartJob.JobID,
        StartJob.StopTime, ValidationResponse.StatusCode,
        ValidationResponse.Message);
  } catch (std::exception &E) {
    Logger::Critical("Unable to process start command, error was: {}",
                     E.what());
  }
}

CmdResponse Handler::startWritingProcess(const FileWriter::Msg &command_message,
                                         StartMessage &start_message) {
  if (std::string exception_message;
      !extractStartMessage(command_message, start_message, exception_message)) {
    return {LogLevel::Warn, 400,
            fmt::format("Failed to extract start command from flatbuffer. The "
                        "error was: {}",
                        exception_message)};
  }

  if (!start_message.ServiceID.empty() &&
      start_message.ServiceID != ServiceId) {
    return {
        LogLevel::Debug, 400,
        fmt::format(
            R"(Rejected start command as the service id was wrong. It should be "{}", it was "{}".)",
            ServiceId, start_message.ServiceID)};
  }

  /// \note This test should never return false as consumption of new jobs
  /// should only be possible when the current one is finished. However, there
  /// is an indication that in some cases jobs will be consumed regardless.
  /// This statement is made 2022-03-21
  if (IsWritingNow()) {
    return {LogLevel::Error, 400,
            fmt::format("Rejected start command as there is "
                        "currently a write job in progress.")};
  }

  if (!isValidUUID(start_message.JobID)) {
    return {
        LogLevel::Warn, 400,
        fmt::format(
            R"(Rejected start command as the job id was invalid (it was: "{}").)",
            start_message.JobID)};
  }

  if (start_message.ControlTopic.empty()) {
    return {LogLevel::Warn, 400,
            fmt::format(R"(Rejected start job as control topic was empty.)")};
  }

  // Start job
  try {
    switchCommandTopic(start_message.ControlTopic, start_message.StartTime);
    DoStart(start_message);
  } catch (std::exception const &E) {
    revertCommandTopic();

    return {LogLevel::Error, 500,
            fmt::format(
                "Failed to start filewriting job. The failure message was: {}",
                E.what())};
  }

  // Success
  return {LogLevel::Info, 201,
          fmt::format("Started write job with start time {} and stop time {}.",
                      toUTCDateTime(start_message.StartTime),
                      toUTCDateTime(start_message.StopTime))};
}

bool extractStopMessage(FileWriter::Msg const &command_message,
                        StopMessage &stop_message, std::string &error_str) {
  try {
    stop_message = Parser::extractStopMessage(command_message);
    return true;
  } catch (std::runtime_error &E) {
    error_str = E.what();
    return false;
  }
}

void Handler::handleStopCommand(FileWriter::Msg command_message) {
  try {
    StopMessage stop_message;

    ActionResult SendResult{ActionResult::Success};
    CmdResponse ValidationResponse =
        stopWritingProcess(command_message, stop_message);
    if (ValidationResponse.StatusCode >= 400) {
      SendResult = ActionResult::Failure;
    }

    Logger::Log(ValidationResponse.LogLevel, ValidationResponse.Message);
    CommandResponse->publishResponse(
        ActionResponse::SetStopTime, SendResult, stop_message.JobID,
        stop_message.CommandID, stop_message.StopTime,
        ValidationResponse.StatusCode, ValidationResponse.Message);
  } catch (std::exception &E) {
    Logger::Critical("Unable to process stop command, error was: {}", E.what());
  }
}

CmdResponse Handler::stopWritingProcess(const FileWriter::Msg &command_message,
                                        StopMessage &stop_message) {
  if (std::string ExceptionMessage;
      !extractStopMessage(command_message, stop_message, ExceptionMessage)) {
    return {LogLevel::Warn, 400,
            fmt::format("Failed to extract stop command from flatbuffer. The "
                        "error was: {}",
                        ExceptionMessage)};
  }

  std::string ResponseMessage;

  if (!(stop_message.ServiceID.empty()) &&
      ServiceId != stop_message.ServiceID) {
    return {
        LogLevel::Debug, 0,
        fmt::format(
            "Rejected stop command as the service ID did not match. Local ID "
            "is {}, command ID was {}.",
            ServiceId, stop_message.ServiceID)};
  }

  if (!IsWritingNow()) {
    return {LogLevel::Warn, 400,
            fmt::format("Rejected stop command as there is "
                        "currently no write job in progress.")};
  }

  if (auto CurrentJobId = GetJobId(); CurrentJobId != stop_message.JobID) {
    return {
        LogLevel::Warn, 400,
        fmt::format("Rejected stop command as the job ID did not match (local"
                    "ID is {}, job ID was: {}).",
                    CurrentJobId, stop_message.JobID)};
  }

  if (!isValidUUID(stop_message.CommandID)) {
    return {LogLevel::Error, 400,
            fmt::format("Rejected stop command as the command ID was invalid "
                        "(it was: {}).",
                        stop_message.CommandID)};
  }

  // Stop job
  try {
    if (stop_message.StopTime == time_point{0ms}) {
      DoStopNow();
      ResponseMessage = "Attempting to stop writing job now.";
    } else {
      DoSetStopTime(stop_message.StopTime);
      ResponseMessage =
          fmt::format("File writing job stop time set to: {}",
                      toUTCDateTime(time_point(stop_message.StopTime)));
    }
  } catch (std::exception const &E) {
    ResponseMessage = E.what();
    return {LogLevel::Error, 500,
            fmt::format("Failed to execute stop command. The "
                        "failure message was: {}",
                        ResponseMessage)};
  }

  // Success
  return {LogLevel::Info, 201, ResponseMessage};
}

} // namespace Command
