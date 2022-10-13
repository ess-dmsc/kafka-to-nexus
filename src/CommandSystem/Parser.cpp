//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file This file defines the different success and failure status that the
/// `StreamController` and the `Streamer` can incur. These error object have
/// some utility methods that can be used to test the more common situations.

#include <6s4t_run_stop_generated.h>
#include <answ_action_response_generated.h>
#include <pl72_run_start_generated.h>
#include <sstream>
#include <wrdn_finished_writing_generated.h>
#include <x5f2_status_generated.h>

#include "Msg.h"
#include "Parser.h"

namespace {
void checkRequiredFieldsArePresent(const RunStart *RunStartData) {
  std::stringstream Errors;
  if (RunStartData->job_id() == nullptr ||
      RunStartData->job_id()->size() == 0) {
    Errors << "Job ID missing, this field is required\n";
  }

  if (RunStartData->nexus_structure() == nullptr ||
      RunStartData->nexus_structure()->size() == 0) {
    Errors << "NeXus Structure missing, this field is "
              "required\n";
  }

  if (RunStartData->filename() == nullptr ||
      RunStartData->filename()->size() == 0) {
    Errors << "Filename missing, this field is required\n";
  }

  if (RunStartData->broker() == nullptr ||
      RunStartData->broker()->size() == 0) {
    Errors << "Broker missing, this field is required\n";
  } else {
    try {
      uri::URI(RunStartData->broker()->str());
    } catch (const std::runtime_error &URIError) {
      Errors << "Unable to parse broker address\n";
    }
  }

  std::string const ErrorsString = Errors.str();
  if (!ErrorsString.empty()) {
    throw std::runtime_error(fmt::format(
        "Errors encountered parsing run start message:\n{}", ErrorsString));
  }
}
} // namespace

namespace Command::Parser {
using FileWriter::Msg;

Command::StartMessage extractStartMessage(Msg const &CommandMessage,
                                          time_point DefaultStartTime) {
  Command::StartMessage Result;

  const auto *const RunStartData = GetRunStart(CommandMessage.data());

  checkRequiredFieldsArePresent(RunStartData);

  if (RunStartData->start_time() > 0) {
    Result.StartTime =
        time_point(std::chrono::milliseconds{RunStartData->start_time()});
  } else {
    Result.StartTime = DefaultStartTime;
  }
  if (RunStartData->stop_time() != 0) {
    Result.StopTime =
        time_point(std::chrono::milliseconds{RunStartData->stop_time()});
  }
  Result.NexusStructure = RunStartData->nexus_structure()->str();
  Result.JobID = RunStartData->job_id()->str();
  if (RunStartData->service_id() != nullptr) {
    Result.ServiceID = RunStartData->service_id()->str();
  }
  Result.BrokerInfo = uri::URI(RunStartData->broker()->str());
  Result.Filename = RunStartData->filename()->str();
  if (RunStartData->metadata() != nullptr) {
    Result.Metadata = RunStartData->metadata()->str();
  }
  if (RunStartData->control_topic() != nullptr) {
    Result.ControlTopic = RunStartData->control_topic()->str();
  }

  return Result;
}

Command::StopMessage extractStopMessage(Msg const &CommandMessage) {
  const auto *const RunStopData = GetRunStop(CommandMessage.data());

  if (RunStopData->job_id() == nullptr || RunStopData->job_id()->size() == 0) {
    throw std::runtime_error("Errors encountered parsing run stop message:\n"
                             "Job ID missing, this field is required");
  }

  StopMessage Result;
  Result.JobID = RunStopData->job_id()->str();
  Result.StopTime =
      time_point(std::chrono::milliseconds{RunStopData->stop_time()});
  Result.CommandID = RunStopData->command_id()->str();
  if (RunStopData->service_id() != nullptr) {
    Result.ServiceID = RunStopData->service_id()->str();
  }

  return Result;
}

bool isStartCommand(Msg const &CommandMessage) {
  auto Verifier =
      flatbuffers::Verifier(CommandMessage.data(), CommandMessage.size());
  return VerifyRunStartBuffer(Verifier) and
         flatbuffers::BufferHasIdentifier(CommandMessage.data(),
                                          RunStartIdentifier());
}

bool isStopCommand(Msg const &CommandMessage) {
  auto Verifier =
      flatbuffers::Verifier(CommandMessage.data(), CommandMessage.size());
  return VerifyRunStopBuffer(Verifier) and
         flatbuffers::BufferHasIdentifier(CommandMessage.data(),
                                          RunStopIdentifier());
}

bool isStatusMessage(Msg const &CommandMessage) {
  auto Verifier =
      flatbuffers::Verifier(CommandMessage.data(), CommandMessage.size());
  return VerifyStatusBuffer(Verifier) and
         flatbuffers::BufferHasIdentifier(CommandMessage.data(),
                                          StatusIdentifier());
}

bool isAnswerMessage(Msg const &CommandMessage) {
  auto Verifier =
      flatbuffers::Verifier(CommandMessage.data(), CommandMessage.size());
  return VerifyActionResponseBuffer(Verifier) and
         flatbuffers::BufferHasIdentifier(CommandMessage.data(),
                                          ActionResponseIdentifier());
}

bool isWritingDoneMessage(Msg const &CommandMessage) {
  auto Verifier =
      flatbuffers::Verifier(CommandMessage.data(), CommandMessage.size());
  return VerifyFinishedWritingBuffer(Verifier) and
         flatbuffers::BufferHasIdentifier(CommandMessage.data(),
                                          FinishedWritingIdentifier());
}

bool isFileWriterHeartbeatMessage(Msg const &CommandMessage) {
  std::string SchemaId(reinterpret_cast<char const *>(CommandMessage.data()) + 4, 4);
  return SchemaId == "x5f2";
}
} // namespace Command::Parser
