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
#include <trompeloeil.hpp>
#include <uuid.h>

using namespace Command;

bool isErrorResponse(const CmdResponse &response) {
  return response.StatusCode >= 400;
}

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

class HandlerStandIn : public Handler {
public:
  // Inherit constructors
  using Handler::Handler;
  // Expose some members as public
  using Handler::validateStartCommandMessage;
};

class HandlerTest : public ::testing::Test {
protected:
  std::unique_ptr<JobListenerMock> jobListenerMock_;
  std::unique_ptr<CommandListenerMock> commandListenerMock_;
  std::unique_ptr<FeedbackProducerMock> feedbackProducerMock_;
  std::unique_ptr<HandlerStandIn> handlerUnderTest_;
  StartMessage startMessage_;
  std::string serviceId_ = "service_id_123";

  void SetUp() override {
    jobListenerMock_ = std::make_unique<JobListenerMock>(
        uri::URI("localhost:1111/no_topic_here"), Kafka::BrokerSettings{});
    commandListenerMock_ = std::make_unique<CommandListenerMock>(
        uri::URI("localhost:1111/no_topic_here"), Kafka::BrokerSettings{});
    feedbackProducerMock_ = std::make_unique<FeedbackProducerMock>();

    handlerUnderTest_ = std::make_unique<HandlerStandIn>(
        serviceId_, Kafka::BrokerSettings{},
        uri::URI("localhost:1111/no_topic_here"), std::move(jobListenerMock_),
        std::move(commandListenerMock_), std::move(feedbackProducerMock_));
    handlerUnderTest_->registerIsWritingFunction(
        []() -> bool { return false; });
    handlerUnderTest_->registerStartFunction(
        []([[maybe_unused]] auto startMessage) -> void {});

    // Use a valid JobID in the base start message
    startMessage_.JobID = "123e4567-e89b-12d3-a456-426614174000";
  }
};

TEST_F(HandlerTest, validateStartCommandReturnsErrorIfAlreadyWriting) {
  handlerUnderTest_->registerIsWritingFunction([]() -> bool { return true; });
  for (bool isPoolCommand : {false, true}) {
    CmdResponse cmdResponse = handlerUnderTest_->validateStartCommandMessage(
        startMessage_, isPoolCommand);
    EXPECT_TRUE(cmdResponse.SendResponse);
    EXPECT_TRUE(isErrorResponse(cmdResponse));
  }
}

TEST_F(HandlerTest, validateStartCommandFromJobPoolAndEmptyServiceId) {
  CmdResponse cmdResponse =
      handlerUnderTest_->validateStartCommandMessage(startMessage_, true);
  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
}

TEST_F(HandlerTest, validateStartCommandFromJobPoolAndMismatchingServiceId) {
  startMessage_.ServiceID = "another_service_id";
  CmdResponse cmdResponse =
      handlerUnderTest_->validateStartCommandMessage(startMessage_, true);
  EXPECT_FALSE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(HandlerTest, validateStartCommandFromJobPoolAndMatchingServiceId) {
  startMessage_.ServiceID = serviceId_;
  CmdResponse cmdResponse =
      handlerUnderTest_->validateStartCommandMessage(startMessage_, true);
  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
}

TEST_F(HandlerTest, validateStartCommandRejectsControlTopicIfNotFromJobPool) {
  startMessage_.ControlTopic = "some_topic";
  CmdResponse cmdResponse =
      handlerUnderTest_->validateStartCommandMessage(startMessage_, false);
  EXPECT_FALSE(handlerUnderTest_->isUsingAlternativeTopic());
  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(HandlerTest, validateStartCommandAcceptsControlTopicIfFromJobPool) {
  EXPECT_FALSE(handlerUnderTest_->isUsingAlternativeTopic());
  startMessage_.ControlTopic = "some_topic";
  CmdResponse cmdResponse =
      handlerUnderTest_->validateStartCommandMessage(startMessage_, true);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
  EXPECT_TRUE(handlerUnderTest_->isUsingAlternativeTopic());
}

TEST_F(HandlerTest, validateStartCommandAcceptsValidJobID) {
  startMessage_.JobID = "321e4567-e89b-12d3-a456-426614174000";
  CmdResponse cmdResponse =
      handlerUnderTest_->validateStartCommandMessage(startMessage_, true);
  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
}

TEST_F(HandlerTest, validateStartCommandRejectsInvalidJobID) {
  startMessage_.JobID = "123";
  CmdResponse cmdResponse =
      handlerUnderTest_->validateStartCommandMessage(startMessage_, true);
  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(HandlerTest, validateStartCommandReportsExceptionUponJobStart) {
  handlerUnderTest_->registerStartFunction(
      []([[maybe_unused]] auto startMessage) -> void {
        throw std::runtime_error("Some error");
      });
  CmdResponse cmdResponse =
      handlerUnderTest_->validateStartCommandMessage(startMessage_, true);
  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(HandlerTest, validateStartCommandSuccessfulStartReturnsResponse) {
  CmdResponse cmdResponse =
      handlerUnderTest_->validateStartCommandMessage(startMessage_, true);
  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
  EXPECT_EQ(cmdResponse.StatusCode, 201);
}