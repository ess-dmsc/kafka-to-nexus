// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandSystem/FeedbackProducerBase.h"
#include "CommandSystem/Handler.h"
#include <gtest/gtest.h>

using namespace Command;

class JobListenerMock : public JobListener {
public:
  JobListenerMock(uri::URI jobPoolUri, Kafka::BrokerSettings settings)
      : JobListener(std::move(jobPoolUri), std::move(settings)) {}

  std::pair<Kafka::PollStatus, Msg> pollForJob() override {
    return {Kafka::PollStatus::TimedOut, Msg()};
  }

  void disconnectFromPool() override {}

  bool isConnected() const override { return false; }
};

class FeedbackProducerMock : public FeedbackProducerBase {
public:
  // Inherit constructors from FeedbackProducerBase
  using FeedbackProducerBase::FeedbackProducerBase;

  void publishResponse([[maybe_unused]] ActionResponse command,
                       [[maybe_unused]] ActionResult result,
                       [[maybe_unused]] std::string jobId,
                       [[maybe_unused]] std::string commandId,
                       [[maybe_unused]] time_point stopTime,
                       [[maybe_unused]] int statusCode,
                       [[maybe_unused]] std::string description) override {}

  void publishStoppedMsg([[maybe_unused]] ActionResult result,
                         [[maybe_unused]] std::string jobId,
                         [[maybe_unused]] std::string description,
                         [[maybe_unused]] std::filesystem::path filePath,
                         [[maybe_unused]] std::string metadata) override {}
};

class CommandListenerMock : public CommandListener {
public:
  CommandListenerMock(uri::URI commandTopicUri, Kafka::BrokerSettings settings)
      : CommandListener(std::move(commandTopicUri), std::move(settings)) {}

  CommandListenerMock(uri::URI commandTopicUri, Kafka::BrokerSettings settings,
                      time_point startTimestamp)
      : CommandListener(std::move(commandTopicUri), std::move(settings),
                        startTimestamp) {}

  std::pair<Kafka::PollStatus, Msg> pollForCommand() override {
    return {Kafka::PollStatus::TimedOut, Msg()};
  }
};

class HandlerTest : public ::testing::Test {
protected:
  std::unique_ptr<JobListenerMock> jobListenerMock_;
  std::unique_ptr<CommandListenerMock> commandListenerMock_;
  std::unique_ptr<FeedbackProducerMock> feedbackProducerMock_;
  std::unique_ptr<Handler> handlerUnderTest_;

  void SetUp() override {
    jobListenerMock_ = std::make_unique<JobListenerMock>(
        uri::URI("localhost:1111/no_topic_here"), Kafka::BrokerSettings{});
    commandListenerMock_ = std::make_unique<CommandListenerMock>(
        uri::URI("localhost:1111/no_topic_here"), Kafka::BrokerSettings{});
    feedbackProducerMock_ = std::make_unique<FeedbackProducerMock>();

    handlerUnderTest_ = std::make_unique<Handler>(
        "ServiceIdentifier", Kafka::BrokerSettings{},
        uri::URI("localhost:1111/no_topic_here"), std::move(jobListenerMock_),
        std::move(commandListenerMock_), std::move(feedbackProducerMock_));
  }
};

TEST_F(HandlerTest, handleStartCommand) {}

TEST_F(HandlerTest, handleStopCommand) {}
