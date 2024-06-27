// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Kafka/MetaDataQuery.h"
#include "Kafka/MetadataException.h"
#include "MainOpt.h"
#include "Master.h"
#include "Status/StatusReporter.h"
#include "Status/StatusService.h"
#include "logger.h"
#include <csignal>
#include <string>

enum class RunStates {
  Running,
  Stopping,
  SIGINT_Received,
  SIGINT_Waiting,
  SIGINT_KafkaWait,
  SIGHUP_Received,
};

void signal_handler(int Signal, std::atomic<RunStates> &RunState) {
  std::string CtrlCString{"Got SIGINT (Ctrl-C). Shutting down gracefully. "
                          "Press Ctrl-C again to shutdown quickly."};
  std::string SIGTERMString{"Got SIGTERM. Shutting down."};
  std::string SIGHUPString{
      "Got SIGHUP. Shutdown will be performed when file-writing is idle."};
  std::string UnknownSignal{"Got unknown signal. Ignoring."};
  switch (Signal) {
  case SIGINT:
    if (RunState == RunStates::Running ||
        RunState == RunStates::SIGHUP_Received) {
      LOG_INFO(CtrlCString);
      RunState = RunStates::SIGINT_Received;
    } else {
      LOG_INFO("Got repeated Ctrl-c. Shutting down now;");
      RunState = RunStates::Stopping;
    }
    break;
  case SIGTERM:
    LOG_INFO(SIGTERMString);
    RunState = RunStates::Stopping;
    break;
  case SIGHUP:
    if (RunState == RunStates::Running) {
      LOG_INFO(SIGHUPString);
      RunState = RunStates::SIGHUP_Received;
    } else {
      LOG_INFO("SIGHUP is only honoured from 'Running' state, ignoring signal "
               "received while on state {}",
               static_cast<int>(RunState.load()));
    }
    break;
  default:
    LOG_INFO(UnknownSignal);
  }
}

bool shouldStop(std::unique_ptr<FileWriter::Master> &MasterPtr,
                bool FindTopicMode, std::atomic<RunStates> &RunState) {
  static time_point SIGINTStart;
  duration WaitForStop{5s};
  if (RunState == RunStates::Stopping) {
    return true;
  }
  if (RunState == RunStates::SIGINT_Received) {
    if (FindTopicMode) {
      return true;
    } else if (not MasterPtr->writingIsFinished()) {
      MasterPtr->stopNow();
      RunState = RunStates::SIGINT_Waiting;
      SIGINTStart = system_clock::now();
    } else {
      return true;
    }
  } else if (RunState == RunStates::SIGINT_Waiting) {
    if (system_clock::now() > SIGINTStart + WaitForStop) {
      LOG_INFO("Failed to shut down gracefully. Stopping now.");
      return true;
    } else if (MasterPtr->writingIsFinished()) {
      RunState = RunStates::SIGINT_KafkaWait;
      return true;
    }
  } else if (RunState == RunStates::SIGHUP_Received) {
    if (FindTopicMode) {
      return true;
    } else if (MasterPtr->writingIsFinished()) {
      RunState = RunStates::SIGINT_KafkaWait;
      return true;
    }
  }
  return false;
}
