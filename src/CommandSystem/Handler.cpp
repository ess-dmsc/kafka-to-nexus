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
  if (not IsWritingNow()) {
    auto JobMsg = JobPool->pollForJob();
    if (JobMsg.first == Kafka::PollStatus::Message) {
      handleCommand(std::move(JobMsg.second), true);
    }
  } else if (JobPool->isConnected()) {
    JobPool->disconnectFromPool();
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

void Handler::registerIsWritingFunction(IsWritingFuncType IsWritingFunction) {
  IsWritingNow = IsWritingFunction;
}

void Handler::registerGetJobIdFunction(GetJobIdFuncType GetJobIdFunction) {
  GetJobId = GetJobIdFunction;
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

void Handler::sendHasStoppedMessage(std::filesystem::path const &FilePath,
                                    nlohmann::json Metadata) {
  Metadata["hdf_structure"] = NexusStructure;
  LOG_DEBUG("Sending FinishedWriting message (Result={} JobId={} File={})",
            "Success", GetJobId(), FilePath.string());
  CommandResponse->publishStoppedMsg(ActionResult::Success, GetJobId(), "",
                                     FilePath, Metadata.dump());
  revertCommandTopic();
}

void Handler::sendErrorEncounteredMessage(std::string const &FileName,
                                          std::string const &Metadata,
                                          std::string const &ErrorMessage) {
  LOG_DEBUG("Sending FinishedWriting message (Result={} JobId={} File={}): {}",
            "Failure", GetJobId(), FileName, ErrorMessage);
  CommandResponse->publishStoppedMsg(ActionResult::Failure, GetJobId(),
                                     ErrorMessage, FileName, Metadata);
}

void Handler::handleCommand(FileWriter::Msg CommandMsg, bool IsJobPoolCommand) {
  if (Parser::isStartCommand(CommandMsg)) {
    handleStartCommand(std::move(CommandMsg), IsJobPoolCommand);
  } else if (Parser::isStopCommand(CommandMsg)) {
    handleStopCommand(std::move(CommandMsg));
  } else if (Parser::isStatusMessage(CommandMsg) or
             Parser::isAnswerMessage(CommandMsg) or
             Parser::isWritingDoneMessage(CommandMsg) or
             Parser::isFileWriterHeartbeatMessage(CommandMsg)) {
    // Do nothing
  } else if (CommandMsg.size() < 8) {
    LOG_TRACE("Unable to handle message as it was too short ({} bytes).",
              CommandMsg.size());
  } else {
    std::string SchemaId(reinterpret_cast<char const *>(CommandMsg.data()) + 4,
                         4);
    LOG_TRACE("Unable to handle (command) message of type: {}", SchemaId);
  }
}

using LogLevel = Log::Severity;

struct CmdResponse {
  Log::Severity LogLevel;
  int StatusCode{0};
  bool SendResponse;
  std::function<std::string()> MessageString;
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
    auto const Id = uuids::uuid::from_string(UUIDStr);
    return not Id->is_nil() and Id->version() != uuids::uuid_version::none and
           Id->variant() == uuids::uuid_variant::rfc;
  } catch (std::system_error const &) {
    return false;
  }
  return false;
}

/// Warn if the message time differs significantly to the current time.
///
/// It does not necessarily mean something is wrong, it could be
/// that an old file is being rewritten, for example.
///
/// \param MsgTime
void checkMsgTimeStampAgainstWallClock(time_point MsgTime) {
  if (system_clock::now() > MsgTime + 15s) {
    LOG_WARN(fmt::format("Start command's timestamp may be bad (it was: {}, "
                         "current time: {}).",
                         toUTCDateTime(MsgTime),
                         toUTCDateTime(system_clock::now())));
  }
}

void Handler::handleStartCommand(FileWriter::Msg CommandMsg,
                                 bool IsJobPoolCommand) {
  try {
    std::string ExceptionMessage;
    StartMessage StartJob;
    std::vector<std::pair<std::function<bool()>, CmdResponse>> CommandSteps;
    CommandSteps.push_back(
        {[&]() {
           return extractStartMessage(CommandMsg, StartJob, ExceptionMessage);
         },
         {LogLevel::Warning, 0, false, [&]() {
            return fmt::format(
                "Failed to extract start command from flatbuffer. The "
                "error was: {}",
                ExceptionMessage);
          }}});

    CommandSteps.push_back(
        {[&]() {
           return not(IsJobPoolCommand xor (StartJob.ServiceID != ServiceId));
         },
         {LogLevel::Debug, 0, false, [&]() {
            return fmt::format(
                R"(Rejected start command as the service id was wrong. It should be "{}", it was "{}".)",
                ServiceId, StartJob.ServiceID);
          }}});

    /// \note This test should never return false as consumption of new jobs
    /// should only be possible when the current one is finished. However, there
    /// is an indication that in some cases jobs will be consumed regardless.
    /// This statement is made 2022-03-21
    CommandSteps.push_back({[&]() { return not IsWritingNow(); },
                            {LogLevel::Error, 400, true, [&]() {
                               return fmt::format(
                                   "Rejected start command as there is "
                                   "currently a write job in progress.");
                             }}});

    CommandSteps.push_back(
        {[&]() { return isValidUUID(StartJob.JobID); },
         {LogLevel::Warning, 400, true, [&]() {
            return fmt::format(
                R"(Rejected start command as the job id was invalid (it was: "{}").)",
                StartJob.JobID);
          }}});

    CommandSteps.push_back(
        {[&]() {
           if (not StartJob.ControlTopic.empty()) {
             if (IsJobPoolCommand) {
               LOG_INFO(
                   R"(Connecting to an alternative command topic "{}" with starting offset "{}")",
                   StartJob.ControlTopic, StartJob.StartTime);
               CommandSource = std::make_unique<CommandListener>(
                   uri::URI{CommandTopicAddress, StartJob.ControlTopic},
                   KafkaSettings, StartJob.StartTime);
               AltCommandResponse = std::make_unique<FeedbackProducer>(
                   ServiceId,
                   uri::URI{CommandTopicAddress, StartJob.ControlTopic},
                   KafkaSettings);
               std::swap(CommandResponse, AltCommandResponse);
               UsingAltTopic = true;
             } else {
               return false;
             }
           }
           return true;
         },
         {LogLevel::Error, 400, true, [&]() {
            return fmt::format(
                R"(Rejected new/alternative command topic ("{}") as the job was not received from job pool.)",
                StartJob.ControlTopic);
          }}});

    CommandSteps.push_back(
        {[&]() {
           try {
             DoStart(StartJob);
             NexusStructure = StartJob.NexusStructure;
           } catch (std::exception const &E) {
             ExceptionMessage = E.what();
             return false;
           }
           return true;
         },
         {LogLevel::Error, 500, true, [&]() {
            return fmt::format(
                "Failed to start filewriting job. The failure message was: {}",
                ExceptionMessage);
          }}});

    ActionResult SendResult{ActionResult::Success};
    CmdResponse OutcomeValue{
        LogLevel::Info, 201, true, [&]() {
          return fmt::format(
              "Started write job with start time {} and stop time {}.",
              toUTCDateTime(StartJob.StartTime),
              toUTCDateTime(StartJob.StopTime));
        }};

    for (auto const &Step : CommandSteps) {
      // cppcheck-suppress useStlAlgorithm
      if (not Step.first()) {
        OutcomeValue = Step.second;
        SendResult = ActionResult::Failure;
        revertCommandTopic();
        break;
      }
    }

    checkMsgTimeStampAgainstWallClock(CommandMsg.getMetaData().timestamp());

    Log::Msg(OutcomeValue.LogLevel, OutcomeValue.MessageString());
    if (OutcomeValue.SendResponse) {
      CommandResponse->publishResponse(
          ActionResponse::StartJob, SendResult, StartJob.JobID, StartJob.JobID,
          StartJob.StopTime, OutcomeValue.StatusCode,
          OutcomeValue.MessageString());
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
         {LogLevel::Warning, 0, false, [&]() {
            return fmt::format(
                "Failed to extract stop command from flatbuffer. The "
                "error was: {}",
                ResponseMessage);
          }}});

    CommandSteps.push_back(
        {[&]() {
           return StopCmd.ServiceID.empty() || ServiceId == StopCmd.ServiceID;
         },
         {LogLevel::Debug, 0, false, [&]() {
            return fmt::format(
                "Rejected stop command as the service id was wrong. It "
                "should be {}, it was {}.",
                ServiceId, StopCmd.ServiceID);
          }}});

    CommandSteps.push_back(
        {[&]() { return IsWritingNow(); },
         {LogLevel::Warning, 400,
          !StopCmd.ServiceID.empty() && ServiceId == StopCmd.ServiceID, [&]() {
            return fmt::format("Rejected stop command as there is "
                               "currently no write job in progress.");
          }}});

    CommandSteps.push_back(
        {[&]() { return GetJobId() == StopCmd.JobID; },
         {LogLevel::Warning, 400,
          !StopCmd.ServiceID.empty() && ServiceId == StopCmd.ServiceID, [&]() {
            return fmt::format(
                "Rejected stop command as the job id was invalid (It "
                "should be {}, it was: {}).",
                GetJobId(), StopCmd.JobID);
          }}});

    CommandSteps.push_back(
        {[&]() { return isValidUUID(StopCmd.CommandID); },
         {LogLevel::Error, 400, true, [&]() {
            return fmt::format(
                "Rejected stop command as the command id was invalid "
                "(it was: {}).",
                StopCmd.CommandID);
          }}});

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
         {LogLevel::Error, 500, true, [&]() {
            return fmt::format(
                "Failed to execute stop command. The failure message was: {}",
                ResponseMessage);
          }}});
    ActionResult SendResult{ActionResult::Success};

    CmdResponse OutcomeValue{LogLevel::Info, 201, true,
                             [&]() { return ResponseMessage; }};
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
