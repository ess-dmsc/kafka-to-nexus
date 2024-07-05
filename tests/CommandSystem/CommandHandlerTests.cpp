// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandSystem/FeedbackProducer.h"
#include "CommandSystem/Handler.h"
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

using namespace Command;

bool isErrorResponse(const CmdResponse &response) {
  return response.StatusCode >= 400;
}

class JobListenerMock : public JobListener {
public:
  JobListenerMock(uri::URI jobPoolUri, Kafka::BrokerSettings settings)
      : JobListener(std::move(jobPoolUri), std::move(settings),
                    std::make_shared<Kafka::StubConsumerFactory>()) {}

  std::pair<Kafka::PollStatus, Msg> pollForJob() override {
    return {Kafka::PollStatus::TimedOut, Msg()};
  }

  void disconnectFromPool() override {}

  bool isConnected() const override { return false; }
};

class HandlerStandIn : public Handler {
public:
  // Inherit constructors
  using Handler::Handler;
  // Expose some members as public
  using Handler::startWriting;
  using Handler::stopWriting;
};

class StartHandlerTest : public ::testing::Test {
protected:
  std::unique_ptr<JobListenerMock> _jobListenerMock;
  std::unique_ptr<CommandListener> _command_listener;
  std::unique_ptr<FeedbackProducer> _feedback_producer;
  std::unique_ptr<HandlerStandIn> _handlerUnderTest;
  StartMessage _startMessage;
  std::string _serviceId = "service_id_123";

  void SetUp() override {
    _jobListenerMock = std::make_unique<JobListenerMock>(
        uri::URI("localhost:1111/no_topic_here"), Kafka::BrokerSettings{});
    _command_listener = CommandListener::create_null(
        uri::URI("localhost:1111/no_topic_here"), Kafka::BrokerSettings{},
        std::make_shared<Kafka::StubConsumerFactory>(), time_point::max());
    auto producer_topic =
        std::make_unique<Kafka::StubProducerTopic>("my_topic");
    _feedback_producer = FeedbackProducer::create_null(
        "my_service_id", std::move(producer_topic));
    _handlerUnderTest = std::make_unique<HandlerStandIn>(
        _serviceId, Kafka::BrokerSettings{},
        uri::URI("localhost:1111/no_topic_here"), std::move(_jobListenerMock),
        std::move(_command_listener), std::move(_feedback_producer));
    _handlerUnderTest->registerIsWritingFunction(
        []() -> bool { return false; });
    _handlerUnderTest->registerStartFunction(
        []([[maybe_unused]] auto startMessage) -> void {});

    // Use a valid JobID in the base start message
    _startMessage.JobID = "123e4567-e89b-12d3-a456-426614174000";
  }
};

TEST_F(StartHandlerTest, validateStartCommandReturnsErrorIfAlreadyWriting) {
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });

  for (bool isPoolCommand : {false, true}) {
    CmdResponse cmdResponse =
        _handlerUnderTest->startWriting(_startMessage, isPoolCommand);
    EXPECT_TRUE(cmdResponse.SendResponse);
    EXPECT_TRUE(isErrorResponse(cmdResponse));
  }
}

TEST_F(StartHandlerTest, validateStartCommandFromJobPoolAndEmptyServiceId) {
  CmdResponse cmdResponse =
      _handlerUnderTest->startWriting(_startMessage, true);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
}

TEST_F(StartHandlerTest,
       validateStartCommandFromJobPoolAndMismatchingServiceId) {
  _startMessage.ServiceID = "another_service_id";

  CmdResponse cmdResponse =
      _handlerUnderTest->startWriting(_startMessage, true);

  EXPECT_FALSE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(StartHandlerTest, validateStartCommandFromJobPoolAndMatchingServiceId) {
  _startMessage.ServiceID = _serviceId;

  CmdResponse cmdResponse =
      _handlerUnderTest->startWriting(_startMessage, true);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
}

TEST_F(StartHandlerTest,
       validateStartCommandRejectsControlTopicIfNotFromJobPool) {
  _startMessage.ControlTopic = "some_topic";

  CmdResponse cmdResponse =
      _handlerUnderTest->startWriting(_startMessage, false);

  EXPECT_FALSE(_handlerUnderTest->isUsingAlternativeTopic());
  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(StartHandlerTest, validateStartCommandAcceptsControlTopicIfFromJobPool) {
  EXPECT_FALSE(_handlerUnderTest->isUsingAlternativeTopic());
  _startMessage.ControlTopic = "some_topic";

  CmdResponse cmdResponse =
      _handlerUnderTest->startWriting(_startMessage, true);

  EXPECT_FALSE(isErrorResponse(cmdResponse));
  EXPECT_TRUE(_handlerUnderTest->isUsingAlternativeTopic());
}

TEST_F(StartHandlerTest, validateStartCommandAcceptsValidJobID) {
  _startMessage.JobID = "321e4567-e89b-12d3-a456-426614174000";

  CmdResponse cmdResponse =
      _handlerUnderTest->startWriting(_startMessage, true);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
}

TEST_F(StartHandlerTest, validateStartCommandRejectsInvalidJobID) {
  _startMessage.JobID = "123";

  CmdResponse cmdResponse =
      _handlerUnderTest->startWriting(_startMessage, true);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(StartHandlerTest, validateStartCommandReportsExceptionUponJobStart) {
  _handlerUnderTest->registerStartFunction(
      []([[maybe_unused]] auto startMessage) -> void {
        throw std::runtime_error("Some error");
      });

  CmdResponse cmdResponse =
      _handlerUnderTest->startWriting(_startMessage, true);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(StartHandlerTest, validateStartCommandSuccessfulStartReturnsResponse) {
  CmdResponse cmdResponse =
      _handlerUnderTest->startWriting(_startMessage, true);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
  EXPECT_EQ(cmdResponse.StatusCode, 201);
}

class StopHandlerTest : public ::testing::Test {
protected:
  std::unique_ptr<JobListenerMock> _jobListenerMock;
  std::unique_ptr<CommandListener> _command_listener;
  std::unique_ptr<FeedbackProducer> _feedback_producer;
  std::unique_ptr<HandlerStandIn> _handlerUnderTest;
  StopMessage _stopMessage;
  std::string _serviceId = "service_id_123";

  void SetUp() override {
    _jobListenerMock = std::make_unique<JobListenerMock>(
        uri::URI("localhost:1111/no_topic_here"), Kafka::BrokerSettings{});
    _command_listener = CommandListener::create_null(
        uri::URI("localhost:1111/no_topic_here"), Kafka::BrokerSettings{},
        std::make_shared<Kafka::StubConsumerFactory>(), time_point::max());
    auto producer_topic =
        std::make_unique<Kafka::StubProducerTopic>("my_topic");
    _feedback_producer = FeedbackProducer::create_null(
        "my_service_id", std::move(producer_topic));
    _handlerUnderTest = std::make_unique<HandlerStandIn>(
        _serviceId, Kafka::BrokerSettings{},
        uri::URI("localhost:1111/no_topic_here"), std::move(_jobListenerMock),
        std::move(_command_listener), std::move(_feedback_producer));
    _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });
    _handlerUnderTest->registerGetJobIdFunction(
        [this]() -> std::string { return this->_stopMessage.JobID; });
    _handlerUnderTest->registerStopNowFunction([]() -> void {});
    _handlerUnderTest->registerSetStopTimeFunction(
        []([[maybe_unused]] auto stopTime) -> void {});

    // Use a valid JobID and CommandID in the base stop message
    _stopMessage.JobID = "123e4567-e89b-12d3-a456-426614174000";
    _stopMessage.CommandID = "321e4567-e89b-12d3-a456-426614174000";
  }
};

TEST_F(StopHandlerTest, validateStopCommandWithNoCurrentJobAndEmptyServiceID) {
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_FALSE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(StopHandlerTest,
       validateStopCommandWithNoCurrentJobAndMatchingServiceID) {
  _stopMessage.ServiceID = _serviceId;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(StopHandlerTest,
       validateStopCommandWithNoCurrentJobAndMismatchingServiceID) {
  _stopMessage.ServiceID = "another_service_id";
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_FALSE(cmdResponse.SendResponse);
  EXPECT_FALSE(
      isErrorResponse(cmdResponse)); // mismatching service ids get tested first
}

TEST_F(StopHandlerTest, validateStopCommandWithMismatchingServiceId) {
  _stopMessage.ServiceID = "another_service_id";

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_FALSE(cmdResponse.SendResponse);
  EXPECT_FALSE(
      isErrorResponse(cmdResponse)); // mismatching service ids get tested first
}

TEST_F(StopHandlerTest, validateStopCommandWithMatchingServiceId) {
  _stopMessage.ServiceID = _serviceId;

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
}

TEST_F(StopHandlerTest,
       validateStopCommandWithMismatchingJobIdAndEmptyServiceID) {
  _handlerUnderTest->registerGetJobIdFunction(
      []() -> std::string { return "different_job_id"; });

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_FALSE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(StopHandlerTest,
       validateStopCommandWithMismatchingJobIdAndMatchingServiceID) {
  _stopMessage.ServiceID = _serviceId;
  _handlerUnderTest->registerGetJobIdFunction(
      []() -> std::string { return "different_job_id"; });

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(StopHandlerTest,
       validateStopCommandWithMismatchingJobIDAndMismatchingServiceID) {
  _stopMessage.ServiceID = "another_service_id";
  _handlerUnderTest->registerGetJobIdFunction(
      []() -> std::string { return "different_job_id"; });

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_FALSE(cmdResponse.SendResponse);
  EXPECT_FALSE(
      isErrorResponse(cmdResponse)); // mismatching service ids get tested first
}

TEST_F(StopHandlerTest, validateStopCommandWithInvalidCommandID) {
  _stopMessage.CommandID = "invalid";

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_TRUE(isErrorResponse(cmdResponse));
}

TEST_F(StopHandlerTest, validateStopCommandImmediateStop) {
  _stopMessage.StopTime = time_point{0ms};

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
  EXPECT_EQ(cmdResponse.StatusCode, 201);
}

TEST_F(StopHandlerTest, validateStopCommandSetStopTime) {
  _stopMessage.StopTime =
      std::chrono::system_clock::now() + std::chrono::minutes(5);

  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);

  EXPECT_TRUE(cmdResponse.SendResponse);
  EXPECT_FALSE(isErrorResponse(cmdResponse));
  EXPECT_EQ(cmdResponse.StatusCode, 201);
}
