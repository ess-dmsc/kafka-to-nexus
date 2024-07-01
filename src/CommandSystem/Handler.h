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
                                     std::string const &Metadata) = 0;
  virtual void sendErrorEncounteredMessage(std::string const &FileName,
                                           std::string const &Metadata,
                                           std::string const &ErrorMessage) = 0;
  virtual void loopFunction() {}
};

class Handler : public HandlerBase {
public:
  static std::unique_ptr<Handler> create(std::string const &ServiceIdentifier,
                                         Kafka::BrokerSettings const &Settings,
                                         const uri::URI &JobPoolUri,
                                         const uri::URI &CommandTopicUri);

  Handler(std::string service_id, Kafka::BrokerSettings settings,
          uri::URI command_topic_uri,
          std::unique_ptr<JobListener> pool_listener,
          std::unique_ptr<CommandListener> command_listener,
          std::unique_ptr<FeedbackProducerBase> command_response);

  void registerStartFunction(StartFuncType StartFunction) override;
  void registerSetStopTimeFunction(StopTimeFuncType StopTimeFunction) override;
  void registerStopNowFunction(StopNowFuncType StopNowFunction) override;
  void registerIsWritingFunction(IsWritingFuncType IsWritingFunction) override;
  void registerGetJobIdFunction(GetJobIdFuncType GetJobIdFunction) override;

  void sendHasStoppedMessage(std::filesystem::path const &FilePath,
                             std::string const &Metadata) override;
  void sendErrorEncounteredMessage(std::string const &FileName,
                                   std::string const &Metadata,
                                   std::string const &ErrorMessage) override;

  void loopFunction() override;
  [[nodiscard]] bool isUsingAlternativeTopic() const { return UsingAltTopic; }

protected:
  /// \brief Initiates writing.
  ///
  /// \param StartJob The start message as a StartMessage struct.
  /// \param IsJobPoolCommand Flag to indicate if the command comes from the job
  /// pool or the command topic.
  /// \return Metadata about the success/failure after processing the command.
  CmdResponse startWriting(StartMessage &StartJob, bool IsJobPoolCommand);

  /// \brief Stops writing.
  ///
  /// \param StopJob The stop message as a StopMessage struct.
  /// \return Metadata about the success/failure after processing the command.
  CmdResponse stopWriting(StopMessage const &StopJob);

private:
  /// \brief Parse a command message and route it to appropriate handling
  /// method.
  ///
  /// \param CommandMsg Kafka message.
  /// \param IsJobPoolCommand Flag to indicate if the command comes from the job
  /// pool or the command topic.
  void handleCommand(FileWriter::Msg CommandMsg, bool IsJobPoolCommand);

  /// \brief Handle start command.
  ///
  /// \param CommandMsg Kafka message.
  /// \param IsJobPoolCommand Flag to indicate if the command comes from the job
  /// pool or the command topic.
  void handleStartCommand(FileWriter::Msg CommandMsg, bool IsJobPoolCommand);

  /// \brief Validate command and start writing.
  ///
  /// \param CommandMsg Kafka message.
  /// \param StartJob Returns the parsed start message as a StartMessage struct.
  /// \param IsJobPoolCommand Flag to indicate if the command comes from the job
  /// pool or the command topic.
  /// \return Metadata about the success/failure after processing the command.
  CmdResponse startWritingProcess(const FileWriter::Msg &CommandMsg,
                                  StartMessage &StartJob,
                                  bool IsJobPoolCommand);

  /// \brief Handle stop command.
  ///
  /// \param CommandMsg Kafka message.
  void handleStopCommand(FileWriter::Msg CommandMsg);

  /// \brief Validate command and start writing.
  ///
  /// \param CommandMsg Kafka message.
  /// \param StopJob Returns the parsed stop message as a StopMessage struct.
  /// \return Metadata about the success/failure after processing the command.
  CmdResponse stopWritingProcess(const FileWriter::Msg &CommandMsg,
                                 StopMessage &StopJob);

  std::string const ServiceId;
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
