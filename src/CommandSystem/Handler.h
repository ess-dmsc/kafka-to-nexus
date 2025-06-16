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
#include "FeedbackProducer.h"
#include "JobListener.h"
#include "Kafka/BrokerSettings.h"
#include "Msg.h"
#include <exception>
#include <functional>
#include <string>

namespace Command {

using StartFuncType = std::function<void(StartMessage)>;
using StopTimeFuncType = std::function<void(time_point)>;
using StopNowFuncType = std::function<void()>;
using IsWritingFuncType = std::function<bool()>;
using GetJobIdFuncType = std::function<std::string()>;

struct CmdResponse {
  LogSeverity LogLevel;
  int StatusCode{0};
  std::string Message;
};

class HandlerBase {
public:
  HandlerBase() = default;
  virtual ~HandlerBase() = default;
  virtual void registerStartFunction(StartFuncType start_function) = 0;
  virtual void
  registerSetStopTimeFunction(StopTimeFuncType set_stop_time_function) = 0;
  virtual void registerStopNowFunction(StopNowFuncType stop_now_function) = 0;
  virtual void
  registerIsWritingFunction(IsWritingFuncType is_writing_function) = 0;
  virtual void
  registerGetJobIdFunction(GetJobIdFuncType get_job_id_function) = 0;

  virtual void sendHasStoppedMessage(std::filesystem::path const &file_path,
                                     std::string const &metadata) = 0;
  virtual void
  sendErrorEncounteredMessage(std::string const &filename,
                              std::string const &metadata,
                              std::string const &error_message) = 0;
  virtual void loopFunction() {}
};

class Handler : public HandlerBase {
public:
  static std::unique_ptr<Handler> create(std::string const &service_id,
                                         Kafka::BrokerSettings const &settings,
                                         std::string const &job_pool_topic,
                                         std::string const &command_topic);

  Handler(std::string service_id, Kafka::BrokerSettings settings,
          std::string command_topic_uri,
          std::unique_ptr<JobListener> pool_listener,
          std::unique_ptr<CommandListener> command_listener,
          std::unique_ptr<FeedbackProducer> command_response);

  void registerStartFunction(StartFuncType start_function) override;
  void
  registerSetStopTimeFunction(StopTimeFuncType set_stop_time_function) override;
  void registerStopNowFunction(StopNowFuncType stop_now_function) override;
  void
  registerIsWritingFunction(IsWritingFuncType is_writing_function) override;
  void registerGetJobIdFunction(GetJobIdFuncType get_job_id_function) override;

  void sendHasStoppedMessage(std::filesystem::path const &file_path,
                             std::string const &meta_data) override;
  void sendErrorEncounteredMessage(std::string const &filename,
                                   std::string const &metadata,
                                   std::string const &error_message) override;

  void loopFunction() override;

private:
  /// \brief Handle start command.
  ///
  /// \param start_message Kafka message.
  /// pool or the command topic.
  void handleStartCommand(FileWriter::Msg start_message);

  /// \brief Validate command and start writing.
  ///
  /// \param command_message Kafka message.
  /// \param start_message Returns the parsed start message as a StartMessage
  /// struct. pool or the command topic.
  /// \return Metadata about the success/failure after processing the command.
  CmdResponse startWritingProcess(const FileWriter::Msg &command_message,
                                  StartMessage &start_message);

  /// \brief Handle stop command.
  ///
  /// \param command_message Kafka message.
  void handleStopCommand(FileWriter::Msg command_message);

  /// \brief Validate command and start writing.
  ///
  /// \param command_message Kafka message.
  /// \param stop_message Returns the parsed stop message as a StopMessage
  /// struct.
  /// \return Metadata about the success/failure after processing the command.
  CmdResponse stopWritingProcess(const FileWriter::Msg &command_message,
                                 StopMessage &stop_message);

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
  void switchCommandTopic(std::string const &control_topic,
                          time_point start_time);

  /// \brief Revert to the default command topic if an alternative topic
  /// has been configured.
  void revertCommandTopic();

  std::unique_ptr<JobListener> JobPool;
  std::unique_ptr<CommandListener> CommandSource;
  std::unique_ptr<FeedbackProducer> CommandResponse;
  std::unique_ptr<FeedbackProducer> AltCommandResponse;
  std::string const CommandTopicAddress;
  Kafka::BrokerSettings const KafkaSettings;
  bool UsingAltTopic{false};
};

} // namespace Command
