// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Master.h"
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
      Logger::Info(CtrlCString);
      RunState = RunStates::SIGINT_Received;
    } else {
      Logger::Info("Got repeated Ctrl-c. Shutting down now;");
      RunState = RunStates::Stopping;
    }
    break;
  case SIGTERM:
    Logger::Info(SIGTERMString);
    RunState = RunStates::Stopping;
    break;
  case SIGHUP:
    if (RunState == RunStates::Running) {
      Logger::Info(SIGHUPString);
      RunState = RunStates::SIGHUP_Received;
    } else {
      Logger::Info(
          "SIGHUP is only honoured from 'Running' state, ignoring signal "
          "received while on state {}",
          static_cast<int>(RunState.load()));
    }
    break;
  default:
    Logger::Info(UnknownSignal);
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
      Logger::Info("Failed to shut down gracefully. Stopping now.");
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