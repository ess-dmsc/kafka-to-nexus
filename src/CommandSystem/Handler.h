// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <string>
#include <exception>
#include <functional>
#include "Commands.h"
#include "Msg.h"
#include "JobListener.h"

namespace Command {

using StartFuncType = std::function<void(StartInfo)>;
using StopTimeFuncType = std::function<void(std::chrono::milliseconds)>;
using StopNowFuncType = std::function<void()>;

class Handler {
public:
  Handler(std::string ServiceIdentifier, uri::URI JobPoolUri, uri::URI CommandUri);

  void registerStartFunction(StartFuncType StartFunction);
  void registerSetStopTimeFunction(StopTimeFuncType StopTimeFunction);
  void registerStopNowFunction(StopNowFuncType StopNowFunction);

  void sendHasStoppedMessage();
  void sendErrorEncounteredMessage([[maybe_unused]] std::string ErrorMessage);

  void loopFunction();

private:
  void handleCommand(FileWriter::Msg CommandMsg, bool IgnoreServiceId);
  std::string const ServiceId;
  std::string JobId;
  StartFuncType DoStart{
      [](auto) { throw std::runtime_error("Not implemented."); }};
  StopTimeFuncType DoSetStopTime{
      [](auto) { throw std::runtime_error("Not implemented."); }};
  StopNowFuncType DoStopNow{
      []() { throw std::runtime_error("Not implemented."); }};

  JobListener JobPool;
};

} //namespace Command
