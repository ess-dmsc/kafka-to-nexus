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
#include "Msg.h"
#include "Parser.h"
#include "TimeUtility.h"
#include <uuid.h>

namespace Command {

Handler::Handler(std::string const &ServiceIdentifier,
                 Kafka::BrokerSettings const &Settings, uri::URI JobPoolUri,
                 uri::URI CommandTopicUri)
    : ServiceId(ServiceIdentifier),
      JobPool(std::make_unique<JobListener>(JobPoolUri, Settings)),
      CommandSource(
          std::make_unique<CommandListener>(CommandTopicUri, Settings)),
      CommandResponse(std::make_unique<FeedbackProducer>(
          ServiceIdentifier, CommandTopicUri, Settings)),
      CommandTopicAddress(CommandTopicUri), KafkaSettings(Settings) {}

void Handler::loopFunction() {
  auto JobMsg = JobPool->pollForJob();
  if (JobMsg.first == Kafka::PollStatus::Message) {
    handleCommand(std::move(JobMsg.second));
  }
  auto CommandMsg = CommandSource->pollForCommand();
  if (CommandMsg.first == Kafka::PollStatus::Message) {
    handleCommand(std::move(CommandMsg.second));
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

void Handler::revertCommandTopic() {
  if (UsingAltTopic) {
    LOG_INFO("Reverting to default command topic: {}",
             CommandTopicAddress.Topic);
    CommandSource =
        std::make_unique<CommandListener>(CommandTopicAddress, KafkaSettings);
    std::swap(AltCommandResponse, CommandResponse);
    UsingAltTopic = false;
  }
}

void Handler::sendHasStoppedMessage(std::string FileName,
                                    nlohmann::json Metadata) {
  Metadata["hdf_structure"] = NexusStructure;
  CommandResponse->publishStoppedMsg(ActionResult::Success, JobId, "", FileName,
                                     Metadata.dump());
  PollForJob = true;
  revertCommandTopic();
}

void Handler::sendErrorEncounteredMessage(std::string FileName,
                                          std::string Metadata,
                                          std::string ErrorMessage) {
  CommandResponse->publishStoppedMsg(ActionResult::Failure, JobId, ErrorMessage,
                                     FileName, Metadata);
  PollForJob = true;
}

void Handler::handleCommand(FileWriter::Msg CommandMsg) {
  if (Parser::isStartCommand(CommandMsg)) {
    handleStartCommand(std::move(CommandMsg));
  } else if (Parser::isStopCommand(CommandMsg)) {
    handleStopCommand(std::move(CommandMsg));
  } else {
    std::string SchemaId(reinterpret_cast<char const *>(CommandMsg.data()) + 4,
                         4);
    LOG_TRACE("Unable to handle (command) message of type: {}", SchemaId);
  }
}

using LogLevel = Log::Severity;

struct CmdResponse {
  Log::Severity LogLevel;
  bool SendResponse;
  std::function<std::string()> MessageString;
  int StatusCode{0};
};

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
    uuids::uuid const Id = uuids::uuid::from_string(UUIDStr);
    return not Id.is_nil() and Id.version() != uuids::uuid_version::none and
           Id.variant() == uuids::uuid_variant::rfc;
  } catch (uuids::uuid_error const &) {
    return false;
  }
  return false;
}

bool isMsgTimeStampValid(time_point MsgTime) {
  if (system_clock::now() < MsgTime + 15s) {
    return true;
  }
  return false;
}

void Handler::handleStartCommand(FileWriter::Msg CommandMsg) {
  try {
    time_point StopTime{0ms};
    std::string ExceptionMessage;
    StartMessage StartJob;
    std::vector<std::pair<std::function<bool()>, CmdResponse>> CommandSteps;
    CommandSteps.push_back(
        {[&]() {
           return extractStartMessage(CommandMsg, StartJob, ExceptionMessage);
         },
         {LogLevel::Warning, false,
          [&]() {
            return fmt::format(
                "Failed to extract start command from flatbuffer. The "
                "error was: {}",
                ExceptionMessage);
          },
          0}});

    CommandSteps.push_back(
        {[&]() { return StartJob.ServiceID != ServiceId; },
         {LogLevel::Debug, false,
          [&]() {
            return fmt::format(
                "Rejected start command as the service id was wrong. It "
                "should be \"{}\", it was \"{}\".",
                ServiceId, StartJob.ServiceID);
          },
          0}});

    CommandSteps.push_back(
        {[&]() { return isValidUUID(StartJob.JobID); },
         {LogLevel::Warning, true,
          [&]() {
            return fmt::format(
                "Rejected start command as the job id was invalid (it "
                "was: \"{}\").",
                StartJob.JobID);
          },
          400}});

    CommandSteps.push_back(
        {[&]() {
           return isMsgTimeStampValid(CommandMsg.getMetaData().timestamp());
         },
         {LogLevel::Warning, true,
          [&]() {
            return fmt::format(
                "Rejected start command as its timestamp was bad (it was: {}, "
                "current time: {}).",
                toUTCDateTime(CommandMsg.getMetaData().timestamp()),
                toUTCDateTime(system_clock::now()));
          },
          400}});

    CommandSteps.push_back(
        {[&]() {
           if (not StartJob.ControlTopic.empty()) {
             LOG_INFO("Connecting to an alternative command topic: \"{}\"",
                      StartJob.ControlTopic);
             CommandSource = std::make_unique<CommandListener>(
                 uri::URI{CommandTopicAddress, StartJob.ControlTopic},
                 KafkaSettings);
             AltCommandResponse = std::make_unique<FeedbackProducer>(
                 ServiceId,
                 uri::URI{CommandTopicAddress, StartJob.ControlTopic},
                 KafkaSettings);
             std::swap(CommandResponse, AltCommandResponse);
             UsingAltTopic = true;
           }
           return true;
         },
         {LogLevel::Error, true,
          [&]() {
            return fmt::format(
                "Rejected new/alternative command topic (\"{}\") as the job "
                "was not received from job pool.",
                StartJob.ControlTopic);
          },
          400}});

    CommandSteps.push_back(
        {[&]() {
           try {
             DoStart(StartJob);
             StopTime = StartJob.StopTime;
             JobId = StartJob.JobID;
             PollForJob = false;
             JobPool->disconnectFromPool();
             NexusStructure = StartJob.NexusStructure;
           } catch (std::exception const &E) {
             PollForJob = true;
             JobId = "not_currently_writing";
             ExceptionMessage = E.what();
             return false;
           }
           return true;
         },
         {LogLevel::Error, true,
          [&]() {
            return fmt::format(
                "Failed to start filewriting job. The failure message was: {}",
                ExceptionMessage);
          },
          500}});

    ActionResult SendResult{ActionResult::Success};
    CmdResponse OutcomeValue{
        LogLevel::Info, true,
        [&]() {
          return fmt::format(
              "Started write job with start time {} and stop time {}.",
              toUTCDateTime(StartJob.StartTime),
              toUTCDateTime(StartJob.StopTime));
        },
        201};
    for (auto const &Step : CommandSteps) {
      // cppcheck-suppress useStlAlgorithm
      if (not Step.first()) {
        OutcomeValue = Step.second;
        SendResult = ActionResult::Failure;
        revertCommandTopic();
        break;
      }
    }

    Log::Msg(OutcomeValue.LogLevel, OutcomeValue.MessageString());
    if (OutcomeValue.SendResponse) {
      CommandResponse->publishResponse(
          ActionResponse::StartJob, SendResult, StartJob.JobID, StartJob.JobID,
          StopTime, OutcomeValue.StatusCode, OutcomeValue.MessageString());
    }
  } catch (std::exception &E) {
    LOG_ERROR("Unable to process start command, error was: {}", E.what());
  }
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
    std::string ResponseMessage;
    StopMessage StopCmd;
    ActionResponse TypeOfAction{ActionResponse::SetStopTime};
    std::vector<std::pair<std::function<bool()>, CmdResponse>> CommandSteps;

    CommandSteps.push_back(
        {[&]() {
           return extractStopMessage(CommandMsg, StopCmd, ResponseMessage);
         },
         {LogLevel::Warning, false,
          [&]() {
            return fmt::format(
                "Failed to extract stop command from flatbuffer. The "
                "error was: {}",
                ResponseMessage);
          },
          0}});

    CommandSteps.push_back(
        {[&]() { return ServiceId == StopCmd.ServiceID; },
         {LogLevel::Debug, false,
          [&]() {
            return fmt::format(
                "Rejected stop command as the service id was wrong. It "
                "should be {}, it was {}.",
                ServiceId, StopCmd.ServiceID);
          },
          0}});

    CommandSteps.push_back({[&]() { return IsWritingNow(); },
                            {LogLevel::Error, true,
                             [&]() {
                               return fmt::format(
                                   "Rejected stop command as there is "
                                   "currently no write job in progress.");
                             },
                             400}});

    CommandSteps.push_back(
        {[&]() { return JobId == StopCmd.JobID; },
         {LogLevel::Warning, true,
          [&]() {
            return fmt::format(
                "Rejected stop command as the job id was invalid (It "
                "should be {}, it was: {}).",
                JobId, StopCmd.JobID);
          },
          400}});

    CommandSteps.push_back(
        {[&]() { return isValidUUID(StopCmd.CommandID); },
         {LogLevel::Error, true,
          [&]() {
            return fmt::format(
                "Rejected stop command as the command id was invalid "
                "(it was: {}).",
                StopCmd.CommandID);
          },
          400}});

    CommandSteps.push_back(
        {[&]() {
           return isMsgTimeStampValid(CommandMsg.getMetaData().timestamp());
         },
         {LogLevel::Warning, true,
          [&]() {
            return fmt::format(
                "Rejected stop command as its timestamp was bad (it was: {}, "
                "current time: {}).",
                toUTCDateTime(CommandMsg.getMetaData().timestamp()),
                toUTCDateTime(system_clock::now()));
          },
          400}});

    CommandSteps.push_back(
        {[&]() {
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
             return false;
           }
           return true;
         },
         {LogLevel::Error, true,
          [&]() {
            return fmt::format(
                "Failed to execute stop command. The failure message was: {}",
                ResponseMessage);
          },
          500}});
    ActionResult SendResult{ActionResult::Success};

    CmdResponse OutcomeValue{LogLevel::Info, true,
                             [&]() { return ResponseMessage; }, 201};
    for (auto const &Step : CommandSteps) {
      // cppcheck-suppress useStlAlgorithm
      if (not Step.first()) {
        OutcomeValue = Step.second;
        SendResult = ActionResult::Failure;
        break;
      }
    }
    Log::Msg(OutcomeValue.LogLevel, OutcomeValue.MessageString());
    if (OutcomeValue.SendResponse) {
      CommandResponse->publishResponse(TypeOfAction, SendResult, StopCmd.JobID,
                                       StopCmd.CommandID, StopCmd.StopTime,
                                       OutcomeValue.StatusCode,
                                       OutcomeValue.MessageString());
    }
  } catch (std::exception &E) {
    LOG_ERROR("Unable to process stop command, error was: {}", E.what());
  }
}

} // namespace Command
