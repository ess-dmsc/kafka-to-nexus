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
#include "IdChecker.h"
#include "Msg.h"
#include "Parser.h"
#include "TimeUtility.h"

namespace Command {

Handler::Handler(std::string const &ServiceIdentifier,
                 Kafka::BrokerSettings const &Settings, uri::URI JobPoolUri,
                 uri::URI CommandTopicUri)
    : Handler(ServiceIdentifier,
              std::make_unique<JobListener>(JobPoolUri, Settings),
              std::make_unique<CommandListener>(CommandTopicUri, Settings),
              std::make_unique<FeedbackProducer>(ServiceIdentifier,
                                                 CommandTopicUri, Settings)) {}

Handler::Handler(std::string ServiceIdentifier,
                 std::unique_ptr<JobListener> JobConsumer,
                 std::unique_ptr<CommandListener> CommandConsumer,
                 std::unique_ptr<FeedbackProducerBase> Response)
    : ServiceId(std::move(ServiceIdentifier)), JobPool(std::move(JobConsumer)),
      CommandSource(std::move(CommandConsumer)),
      CommandResponse(std::move(Response)) {}

void Handler::loopFunction() {
  if (PollForJob and JobPool != nullptr) {
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

void Handler::sendHasStoppedMessage(std::string FileName,
                                    std::string Metadata) {
  CommandResponse->publishStoppedMsg(ActionResult::Success, JobId, "", FileName,
                                     Metadata);
  PollForJob = true;
}

void Handler::sendErrorEncounteredMessage(std::string FileName,
                                          std::string Metadata,
                                          std::string ErrorMessage) {
  CommandResponse->publishStoppedMsg(ActionResult::Failure, JobId, ErrorMessage,
                                     FileName, Metadata);
  PollForJob = true;
}

void Handler::handleCommand(FileWriter::Msg CommandMsg, bool IgnoreServiceId) {
  if (Parser::isStartCommand(CommandMsg)) {
    handleStartCommand(std::move(CommandMsg), IgnoreServiceId);
  } else if (Parser::isStopCommand(CommandMsg)) {
    handleStopCommand(std::move(CommandMsg));
  } else {
    std::string SchemaId(reinterpret_cast<char const *>(CommandMsg.data()) + 4,
                         4);
    LOG_DEBUG("Unable to handle (command) message of type: {}", SchemaId);
  }
}

enum class CmdOutcome {
  FailedAtExtraction,
  FailedAtServiceId,
  FailedAtJobId,
  FailedAtCmdId,
  FailedAtCmd,
  CmdIsDone,
};

using LogLevel = spdlog::level::level_enum;

struct CmdResponse {
  spdlog::level::level_enum LogLevel;
  bool SendResponse;
  std::string MessageString;
  int StatusCode{0};
};

bool extractStartInfo(FileWriter::Msg const &CommandMsg, StartMessage &Msg,
                      std::string &ErrorStr) {
  try {
    Msg = Parser::extractStartInformation(CommandMsg);
    return true;
  } catch (std::runtime_error &E) {
    ErrorStr = E.what();
    return false;
  }
}

void Handler::handleStartCommand(FileWriter::Msg CommandMsg,
                                 bool IgnoreServiceId) {
  try {
    time_point StopTime = time_point::min();
    CmdOutcome Outcome{CmdOutcome::FailedAtExtraction};
    std::string ExceptionMessage;
    StartMessage StartJob;
    if (extractStartInfo(CommandMsg, StartJob, ExceptionMessage)) {
      Outcome = CmdOutcome::FailedAtServiceId;
    }
    if (Outcome == CmdOutcome::FailedAtServiceId and
        not(IgnoreServiceId xor (StartJob.ServiceID != ServiceId))) {
      Outcome = CmdOutcome::FailedAtJobId;
    }
    auto IdValidity = isJobIdValid(StartJob.JobID);
    if (Outcome == CmdOutcome::FailedAtJobId and IdValidity.first) {
      Outcome = CmdOutcome::FailedAtCmd;
    } else if (not IdValidity.first) {
      LOG_ERROR("Job-id verification failure. Failure message was: {}",
                IdValidity.second);
    }
    if (Outcome == CmdOutcome::FailedAtCmd) {
      try {
        DoStart(StartJob);
        StopTime = StartJob.StopTime;
        JobId = StartJob.JobID;
        PollForJob = false;
        JobPool->disconnectFromPool();
        Outcome = CmdOutcome::CmdIsDone;
      } catch (std::exception const &E) {
        PollForJob = true;
        JobId = "";
        ExceptionMessage = E.what();
      }
    }
    std::map<CmdOutcome, CmdResponse> OutcomeMap{
        {{CmdOutcome::FailedAtExtraction},
         {LogLevel::warn, false,
          fmt::format("Failed to extract start command from flatbuffer. The "
                      "error was: {}",
                      ExceptionMessage),
          0}},
        {{CmdOutcome::FailedAtServiceId},
         {LogLevel::debug, false,
          fmt::format("Rejected start command as the service id was wrong. It "
                      "should be {}, it was {}.",
                      ServiceId, StartJob.ServiceID),
          0}},
        {{CmdOutcome::FailedAtJobId},
         {LogLevel::warn, true,
          fmt::format("Rejected start command as the job id was invalid (it "
                      "was: {}). Reason: {}",
                      StartJob.JobID, IdValidity.second),
          400}},
        {{CmdOutcome::FailedAtCmd},
         {LogLevel::err, true,
          fmt::format(
              "Failed to start filewriting job. The failure message was: {}",
              ExceptionMessage),
          500}},
        {{CmdOutcome::CmdIsDone},
         {LogLevel::info, true,
          fmt::format("Started write job with start time {} and stop time {}.",
                      toUTCDateTime(time_point(
                          std::chrono::milliseconds{StartJob.StartTime})),
                      toUTCDateTime(StartJob.StopTime)),
          201}},
    };
    ActionResult SendResult{ActionResult::Failure};
    if (Outcome == CmdOutcome::CmdIsDone) {
      SendResult = ActionResult::Success;
    }
    auto OutcomeValue = OutcomeMap[Outcome];
    getLogger()->log(OutcomeValue.LogLevel, OutcomeValue.MessageString);
    if (OutcomeValue.SendResponse) {
      CommandResponse->publishResponse(
          ActionResponse::StartJob, SendResult, StartJob.JobID, StartJob.JobID,
          StopTime, OutcomeValue.StatusCode, OutcomeValue.MessageString);
    }
  } catch (std::out_of_range const &E) {
    LOG_ERROR("Unknown outcome of start job command");
  } catch (std::exception &E) {
    LOG_ERROR("Unable to process start command, error was: {}", E.what());
  }
}

bool extractStopInfo(FileWriter::Msg const &CommandMsg, StopMessage &Msg,
                     std::string &ErrorStr) {
  try {
    Msg = Parser::extractStopInformation(CommandMsg);
    return true;
  } catch (std::runtime_error &E) {
    ErrorStr = E.what();
    return false;
  }
}

void Handler::handleStopCommand(FileWriter::Msg CommandMsg) {
  try {
    CmdOutcome Outcome{CmdOutcome::FailedAtExtraction};
    std::string ResponseMessage;
    StopMessage StopCmd;
    ActionResponse TypeOfAction{ActionResponse::SetStopTime};
    if (extractStopInfo(CommandMsg, StopCmd, ResponseMessage)) {
      Outcome = CmdOutcome::FailedAtServiceId;
    }
    if (Outcome == CmdOutcome::FailedAtServiceId and
        ServiceId == StopCmd.ServiceID) {
      Outcome = CmdOutcome::FailedAtJobId;
    }
    if (Outcome == CmdOutcome::FailedAtJobId and JobId == StopCmd.JobID) {
      Outcome = CmdOutcome::FailedAtCmdId;
    }
    auto IdValidity = isCmdIdValid(StopCmd.CommandID);
    if (Outcome == CmdOutcome::FailedAtCmdId and IdValidity.first) {
      Outcome = CmdOutcome::FailedAtCmd;
    } else if (not IdValidity.first) {
      LOG_ERROR("Cmd-id verification failure. Failure message was: {}",
                IdValidity.second);
    }
    if (Outcome == CmdOutcome::FailedAtCmd and StopCmd.StopTime == 0ms) {
      try {
        DoStopNow();
        Outcome = CmdOutcome::CmdIsDone;
        ResponseMessage = "Attempting to stop writing job now.";
      } catch (std::exception const &E) {
        ResponseMessage = E.what();
      }
    } else if (Outcome == CmdOutcome::FailedAtCmd) {
      try {
        DoSetStopTime(StopCmd.StopTime);
        Outcome = CmdOutcome::CmdIsDone;
        ResponseMessage =
            fmt::format("File writing job stop time set to: {}",
                        toUTCDateTime(time_point(StopCmd.StopTime)));
      } catch (std::exception const &E) {
        ResponseMessage = E.what();
      }
    }
    std::map<CmdOutcome, CmdResponse> OutcomeMap{
        {{CmdOutcome::FailedAtExtraction},
         {LogLevel::warn, false,
          fmt::format("Failed to extract stop command from flatbuffer. The "
                      "error was: {}",
                      ResponseMessage),
          0}},
        {{CmdOutcome::FailedAtServiceId},
         {LogLevel::debug, false,
          fmt::format("Rejected stop command as the service id was wrong. It "
                      "should be {}, it was {}.",
                      ServiceId, StopCmd.ServiceID),
          0}},
        {{CmdOutcome::FailedAtJobId},
         {LogLevel::warn, true,
          fmt::format("Rejected stop command as the job id was invalid (It "
                      "should be {}, it was: {}).",
                      JobId, StopCmd.JobID),
          400}},
        {{CmdOutcome::FailedAtCmdId},
         {LogLevel::err, true,
          fmt::format("Rejected stop command as the command id was invalid "
                      "(it was: {}).",
                      StopCmd.CommandID),
          400}},
        {{CmdOutcome::FailedAtCmd},
         {LogLevel::err, true,
          fmt::format(
              "Failed to execute stop command. The failure message was: {}",
              ResponseMessage),
          500}},
        {{CmdOutcome::CmdIsDone}, {LogLevel::info, true, ResponseMessage, 202}},
    };
    ActionResult SendResult{ActionResult::Failure};
    if (Outcome == CmdOutcome::CmdIsDone) {
      SendResult = ActionResult::Success;
    }
    auto OutcomeValue = OutcomeMap[Outcome];
    getLogger()->log(OutcomeValue.LogLevel, OutcomeValue.MessageString);
    if (OutcomeValue.SendResponse) {
      CommandResponse->publishResponse(
          TypeOfAction, SendResult, StopCmd.JobID, StopCmd.CommandID,
          time_point(std::chrono::milliseconds{StopCmd.StopTime}),
          OutcomeValue.StatusCode, OutcomeValue.MessageString);
    }
  } catch (std::out_of_range const &E) {
    LOG_ERROR("Unknown outcome of set stop time command");
  } catch (std::exception &E) {
    LOG_ERROR("Unable to process stop command, error was: {}", E.what());
  }
}

} // namespace Command
