// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "CommandListener.h"
#include "Commands.h"
#include "FeedbackProducerBase.h"
#include "JobListener.h"
#include "Kafka/BrokerSettings.h"
#include "Msg.h"
#include <exception>
#include <functional>
#include <string>

namespace Command {

using StartFuncType = std::function<void(StartInfo)>;
using StopTimeFuncType = std::function<void(time_point)>;
using StopNowFuncType = std::function<void()>;
using IsWritingFuncType = std::function<bool()>;

class HandlerBase {
public:
  HandlerBase() = default;
  virtual ~HandlerBase() = default;
  virtual void registerStartFunction(StartFuncType StartFunction) = 0;
  virtual void
  registerSetStopTimeFunction(StopTimeFuncType StopTimeFunction) = 0;
  virtual void registerStopNowFunction(StopNowFuncType StopNowFunction) = 0;
  virtual void
  registerIsWritingFunction(IsWritingFuncType IsWritingFunction) = 0;

  virtual void sendHasStoppedMessage(std::string const &FileName,
                                     nlohmann::json Metadata) = 0;
  virtual void sendErrorEncounteredMessage(std::string const &FileName,
                                           std::string const &Metadata,
                                           std::string const &ErrorMessage) = 0;
  virtual void loopFunction() {}
};

class Handler : public HandlerBase {
public:
  Handler(std::string const &ServiceIdentifier,
          Kafka::BrokerSettings const &Settings, uri::URI JobPoolUri,
          uri::URI CommandTopicUri);
  Handler(std::string ServiceIdentifier,
          std::unique_ptr<JobListener> JobConsumer,
          std::unique_ptr<CommandListener> CommandConsumer,
          std::unique_ptr<FeedbackProducerBase> Response);

  void registerStartFunction(StartFuncType StartFunction) override;
  void registerSetStopTimeFunction(StopTimeFuncType StopTimeFunction) override;
  void registerStopNowFunction(StopNowFuncType StopNowFunction) override;
  void registerIsWritingFunction(IsWritingFuncType IsWritingFunction) override;

  void sendHasStoppedMessage(std::string const &FileName,
                             nlohmann::json Metadata) override;
  void sendErrorEncounteredMessage(std::string const &FileName,
                                   std::string const &Metadata,
                                   std::string const &ErrorMessage) override;

  void loopFunction() override;

private:
  void handleCommand(FileWriter::Msg CommandMsg, bool IsJobPoolCommand);
  void handleStartCommand(FileWriter::Msg CommandMsg, bool IsJobPoolCommand);
  void handleStopCommand(FileWriter::Msg CommandMsg);
  std::string const ServiceId;
  std::string JobId;
  std::string NexusStructure;
  StartFuncType DoStart{
      [](auto) { throw std::runtime_error("Not implemented."); }};
  StopTimeFuncType DoSetStopTime{
      [](auto) { throw std::runtime_error("Not implemented."); }};
  StopNowFuncType DoStopNow{
      []() { throw std::runtime_error("Not implemented."); }};
  IsWritingFuncType IsWritingNow{
      []() -> bool { throw std::runtime_error("Not implemented."); }};

  /// \brief Revert to the default command topic if an alternative topic
  /// has been configured.
  void revertCommandTopic();

  bool PollForJob{true};
  std::unique_ptr<JobListener> JobPool;
  std::unique_ptr<CommandListener> CommandSource;
  std::unique_ptr<FeedbackProducerBase> CommandResponse;
  std::unique_ptr<FeedbackProducerBase> AltCommandResponse;
  uri::URI const CommandTopicAddress;
  Kafka::BrokerSettings const KafkaSettings;
  bool UsingAltTopic{false};
};

} // namespace Command
