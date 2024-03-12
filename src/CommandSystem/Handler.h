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
using GetJobIdFuncType = std::function<std::string()>;

struct CmdResponse {
  Log::Severity LogLevel;
  int StatusCode{0};
  bool SendResponse;
  std::function<std::string()> MessageString;
};

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
  virtual void registerGetJobIdFunction(GetJobIdFuncType GetJobIdFunction) = 0;

  virtual void sendHasStoppedMessage(std::filesystem::path const &FilePath,
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
  Handler(std::string const &ServiceIdentifier,
          Kafka::BrokerSettings const &Settings, uri::URI CommandTopicUri,
          std::unique_ptr<JobListener> JobConsumer,
          std::unique_ptr<CommandListener> CommandConsumer,
          std::unique_ptr<FeedbackProducerBase> Response);

  void registerStartFunction(StartFuncType StartFunction) override;
  void registerSetStopTimeFunction(StopTimeFuncType StopTimeFunction) override;
  void registerStopNowFunction(StopNowFuncType StopNowFunction) override;
  void registerIsWritingFunction(IsWritingFuncType IsWritingFunction) override;
  void registerGetJobIdFunction(GetJobIdFuncType GetJobIdFunction) override;

  void sendHasStoppedMessage(std::filesystem::path const &FilePath,
                             nlohmann::json Metadata) override;
  void sendErrorEncounteredMessage(std::string const &FileName,
                                   std::string const &Metadata,
                                   std::string const &ErrorMessage) override;

  void loopFunction() override;
  bool isUsingAlternativeTopic() const { return UsingAltTopic; }

protected:
  /// \brief Validate payload of start command.
  ///
  /// \param StartJob Returns the parsed start message as a StartMessage struct.
  /// \param IsJobPoolCommand Flag to indicate if the command comes from the job
  /// pool or the command topic.
  /// \return Metadata about the success/failure after processing the command.
  CmdResponse processStart(StartMessage &StartJob, bool IsJobPoolCommand);

private:
  /// \brief Parse a command message and route it to appropriate handling
  /// method.
  ///
  /// \param CommandMsg Kafka message.
  /// \param IsJobPoolCommand Flag to indicate if the command comes from the job
  /// pool or the command topic.
  void handleCommand(FileWriter::Msg CommandMsg, bool IsJobPoolCommand);

  /// \brief Validate and process start command.
  ///
  /// \param CommandMsg Kafka message.
  /// \param IsJobPoolCommand Flag to indicate if the command comes from the job
  /// pool or the command topic.
  void handleStartCommand(FileWriter::Msg CommandMsg, bool IsJobPoolCommand);

  /// \brief Validate start command.
  ///
  /// \param CommandMsg Kafka message.
  /// \param StartJob Returns the parsed start message as a StartMessage struct.
  /// \param IsJobPoolCommand Flag to indicate if the command comes from the job
  /// pool or the command topic.
  /// \return Metadata about the success/failure after processing the command.
  CmdResponse processStartMessage(const FileWriter::Msg &CommandMsg,
                                  StartMessage &StartJob,
                                  bool IsJobPoolCommand);

  /// \brief Validate and process stop command.
  ///
  /// \param CommandMsg Kafka message.
  void handleStopCommand(FileWriter::Msg CommandMsg);

  std::string const ServiceId;
  std::string NexusStructure;
  StartFuncType DoStart{[](auto) {
    throw std::runtime_error("DoStart(): Not set/implemented.");
  }};
  StopTimeFuncType DoSetStopTime{[](auto) {
    throw std::runtime_error("DoSetStopTime(): Not set/implemented.");
  }};
  StopNowFuncType DoStopNow{
      []() { throw std::runtime_error("DoStopNow(): Not set/implemented."); }};
  IsWritingFuncType IsWritingNow{[]() -> bool {
    throw std::runtime_error("IsWritingNow(): Not set/implemented.");
  }};
  GetJobIdFuncType GetJobId{[]() -> std::string {
    throw std::runtime_error("GetJobId(): Not set/implemented.");
  }};

  /// \brief Switch to an alternative command topic.
  void switchCommandTopic(std::string_view ControlTopic,
                          time_point const StartTime);

  /// \brief Revert to the default command topic if an alternative topic
  /// has been configured.
  void revertCommandTopic();

  std::unique_ptr<JobListener> JobPool;
  std::unique_ptr<CommandListener> CommandSource;
  std::unique_ptr<FeedbackProducerBase> CommandResponse;
  std::unique_ptr<FeedbackProducerBase> AltCommandResponse;
  uri::URI const CommandTopicAddress;
  Kafka::BrokerSettings const KafkaSettings;
  bool UsingAltTopic{false};
};

} // namespace Command
