// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandSystem/Handler.h"
#include "helpers/RunStartStopHelpers.h"
#include <answ_action_response_generated.h>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>
#include <uuid.h>

using namespace Command;

std::string const example_json = R"(
{
	"children": [{
		"name": "entry",
		"type": "group",
		"attributes": [{
			"name": "NX_class",
			"dtype": "string",
			"values": "NXentry"
		}],
		"children": [{
				"module": "dataset",
				"config": {
					"name": "title",
					"values": "This is a title",
					"dtype": "string"
				}
			}
		]
	}]
}
                             )";

bool isErrorResponse(const CmdResponse &response) {
  return response.StatusCode >= 400;
}

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
  std::shared_ptr<Kafka::StubConsumerFactory> _consumer_factory;
  std::unique_ptr<Handler> _handlerUnderTest;
  Kafka::StubProducerTopic *_producer_topic;
  StartMessage _startMessage;
  std::string _serviceId = "service_id_123";
  std::string _default_command_topic = "command_topic";

  void SetUp() override {
    _consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
    auto job_listener =
        JobListener::create_null("pool_topic", {}, _consumer_factory);
    auto command_listener = CommandListener::create_null(
        _default_command_topic, Kafka::BrokerSettings{}, _consumer_factory,
        time_point::max());
    auto producer_topic =
        std::make_unique<Kafka::StubProducerTopic>("my_topic");
    _producer_topic = producer_topic.get();

    auto feedback_producer =
        FeedbackProducer::create_null(_serviceId, std::move(producer_topic));
    _handlerUnderTest = std::make_unique<Handler>(
        _serviceId, Kafka::BrokerSettings{}, _default_command_topic,
        std::move(job_listener), std::move(command_listener),
        std::move(feedback_producer));
    _handlerUnderTest->registerIsWritingFunction(
        []() -> bool { return false; });
    _handlerUnderTest->registerStartFunction(
        []([[maybe_unused]] auto startMessage) -> void {});

    // Use a valid JobID in the base start message
    _startMessage.JobID = "123e4567-e89b-12d3-a456-426614174000";
  }
};

TEST_F(StartHandlerTest, start_command_actioned) {
  bool start_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      [&start_called]([[maybe_unused]] auto startMessage) -> void {
        start_called = true;
      });

  auto start_message = RunStartStopHelpers::buildRunStartMessage(
      "my_instrument", "my_run_name", example_json, _startMessage.JobID,
      {_serviceId}, "my_file_name", 123456, 987654, "new_command_topic");
  FileWriter::MessageMetaData meta_data{.topic = "pool_topic"};
  _consumer_factory->messages->emplace_back(start_message.data(),
                                            start_message.size(), meta_data);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(true, start_called);
}

TEST_F(StartHandlerTest, start_command_sends_response) {
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      []([[maybe_unused]] auto startMessage) -> void {});

  // NOTE: using the same command topic, so it doesn't create a new
  // ProducerTopic. This allows us to see the messages
  auto start_message = RunStartStopHelpers::buildRunStartMessage(
      "my_instrument", "my_run_name", example_json, _startMessage.JobID,
      {_serviceId}, "my_file_name", 123456, 987654, _default_command_topic);
  FileWriter::MessageMetaData meta_data{.topic = "pool_topic"};
  _consumer_factory->messages->emplace_back(start_message.data(),
                                            start_message.size(), meta_data);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  auto message = _producer_topic->messages[0].get();
  auto result = GetActionResponse(message->data);

  EXPECT_EQ(1u, _producer_topic->messages.size());
  ASSERT_EQ(result->action(), ActionType::StartJob);
  ASSERT_EQ(result->outcome(), ActionOutcome::Success);
}

TEST_F(StartHandlerTest, start_command_ignored_if_already_writing) {
  bool start_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });
  _handlerUnderTest->registerStartFunction(
      [&start_called]([[maybe_unused]] auto startMessage) -> void {
        start_called = true;
      });

  auto start_message = RunStartStopHelpers::buildRunStartMessage(
      "my_instrument", "my_run_name", example_json, _startMessage.JobID,
      {_serviceId}, "my_file_name", 123456, 987654, "new_control_topic");
  FileWriter::MessageMetaData meta_data{.topic = _default_command_topic};
  _consumer_factory->messages->emplace_back(start_message.data(),
                                            start_message.size(), meta_data);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(false, start_called);
}

TEST_F(StartHandlerTest, start_command_with_no_service_id_starts) {
  bool start_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      [&start_called]([[maybe_unused]] auto startMessage) -> void {
        start_called = true;
      });

  auto start_message = RunStartStopHelpers::buildRunStartMessage(
      "my_instrument", "my_run_name", example_json, _startMessage.JobID, {},
      "my_file_name", 123456, 987654, "new_control_topic");
  FileWriter::MessageMetaData meta_data{.topic = "pool_topic"};
  _consumer_factory->messages->emplace_back(start_message.data(),
                                            start_message.size(), meta_data);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(true, start_called);
}

TEST_F(StartHandlerTest, start_command_with_mismatched_service_id_is_rejected) {
  bool start_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      [&start_called]([[maybe_unused]] auto startMessage) -> void {
        start_called = true;
      });

  auto start_message = RunStartStopHelpers::buildRunStartMessage(
      "my_instrument", "my_run_name", example_json, _startMessage.JobID,
      {"wrong_service_id"}, "my_file_name", 123456, 987654,
      "new_control_topic");
  FileWriter::MessageMetaData meta_data{.topic = "pool_topic"};
  _consumer_factory->messages->emplace_back(start_message.data(),
                                            start_message.size(), meta_data);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(false, start_called);
}

TEST_F(StartHandlerTest, start_command_with_invalid_UUID_is_rejected) {
  bool start_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      [&start_called]([[maybe_unused]] auto startMessage) -> void {
        start_called = true;
      });

  auto start_message = RunStartStopHelpers::buildRunStartMessage(
      "my_instrument", "my_run_name", example_json, "invalid_uuid",
      {"wrong_service_id"}, "my_file_name", 123456, 987654,
      "new_control_topic");
  FileWriter::MessageMetaData meta_data{.topic = "pool_topic"};
  _consumer_factory->messages->emplace_back(start_message.data(),
                                            start_message.size(), meta_data);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(false, start_called);
}

TEST_F(StartHandlerTest, rejected_command_sends_response) {
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      []([[maybe_unused]] auto startMessage) -> void {});

  // NOTE: using the same command topic, so it doesn't create a new
  // ProducerTopic. This allows us to see the messages
  auto start_message = RunStartStopHelpers::buildRunStartMessage(
      "my_instrument", "my_run_name", example_json, "invalid_uuid",
      {_serviceId}, "my_file_name", 123456, 987654, _default_command_topic);
  FileWriter::MessageMetaData meta_data{.topic = "pool_topic"};
  _consumer_factory->messages->emplace_back(start_message.data(),
                                            start_message.size(), meta_data);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  auto message = _producer_topic->messages[0].get();
  auto result = GetActionResponse(message->data);

  EXPECT_EQ(1u, _producer_topic->messages.size());
  ASSERT_EQ(result->action(), ActionType::StartJob);
  ASSERT_EQ(result->outcome(), ActionOutcome::Failure);
}

// class StopHandlerTest : public ::testing::Test {
// protected:
//  std::unique_ptr<JobListenerMock> _jobListenerMock;
//  std::unique_ptr<CommandListenerMock> _commandListenerMock;
//  std::unique_ptr<FeedbackProducer> _feedbackProducer;
//  std::unique_ptr<HandlerStandIn> _handlerUnderTest;
//  StopMessage _stopMessage;
//  std::string _serviceId = "service_id_123";
//
//  void SetUp() override {
//    _jobListenerMock = std::make_unique<JobListenerMock>(
//        "no_topic_here", Kafka::BrokerSettings{});
//    _commandListenerMock = std::make_unique<CommandListenerMock>(
//        "no_topic_here", Kafka::BrokerSettings{}, time_point::max());
//    _feedbackProducer = FeedbackProducer::create_null(
//        _serviceId,
//        std::make_unique<Kafka::StubProducerTopic>("no_topic_here"));
//    _handlerUnderTest = std::make_unique<HandlerStandIn>(
//        _serviceId, Kafka::BrokerSettings{}, "no_topic_here",
//        std::move(_jobListenerMock), std::move(_commandListenerMock),
//        std::move(_feedbackProducer));
//    _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true;
//    }); _handlerUnderTest->registerGetJobIdFunction(
//        [this]() -> std::string { return this->_stopMessage.JobID; });
//    _handlerUnderTest->registerStopNowFunction([]() -> void {});
//    _handlerUnderTest->registerSetStopTimeFunction(
//        []([[maybe_unused]] auto stopTime) -> void {});
//
//    // Use a valid JobID and CommandID in the base stop message
//    _stopMessage.JobID = "123e4567-e89b-12d3-a456-426614174000";
//    _stopMessage.CommandID = "321e4567-e89b-12d3-a456-426614174000";
//  }
//};
//
// TEST_F(StopHandlerTest, validateStopCommandWithNoCurrentJobAndEmptyServiceID)
// {
//  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false;
//  });
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_FALSE(cmdResponse.SendResponse);
//  EXPECT_TRUE(isErrorResponse(cmdResponse));
//}
//
// TEST_F(StopHandlerTest,
//       validateStopCommandWithNoCurrentJobAndMatchingServiceID) {
//  _stopMessage.ServiceID = _serviceId;
//  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false;
//  });
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_TRUE(cmdResponse.SendResponse);
//  EXPECT_TRUE(isErrorResponse(cmdResponse));
//}
//
// TEST_F(StopHandlerTest,
//       validateStopCommandWithNoCurrentJobAndMismatchingServiceID) {
//  _stopMessage.ServiceID = "another_service_id";
//  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false;
//  });
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_FALSE(cmdResponse.SendResponse);
//  EXPECT_FALSE(
//      isErrorResponse(cmdResponse)); // mismatching service ids get tested
//      first
//}
//
// TEST_F(StopHandlerTest, validateStopCommandWithMismatchingServiceId) {
//  _stopMessage.ServiceID = "another_service_id";
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_FALSE(cmdResponse.SendResponse);
//  EXPECT_FALSE(
//      isErrorResponse(cmdResponse)); // mismatching service ids get tested
//      first
//}
//
// TEST_F(StopHandlerTest, validateStopCommandWithMatchingServiceId) {
//  _stopMessage.ServiceID = _serviceId;
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_TRUE(cmdResponse.SendResponse);
//  EXPECT_FALSE(isErrorResponse(cmdResponse));
//}
//
// TEST_F(StopHandlerTest,
//       validateStopCommandWithMismatchingJobIdAndEmptyServiceID) {
//  _handlerUnderTest->registerGetJobIdFunction(
//      []() -> std::string { return "different_job_id"; });
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_FALSE(cmdResponse.SendResponse);
//  EXPECT_TRUE(isErrorResponse(cmdResponse));
//}
//
// TEST_F(StopHandlerTest,
//       validateStopCommandWithMismatchingJobIdAndMatchingServiceID) {
//  _stopMessage.ServiceID = _serviceId;
//  _handlerUnderTest->registerGetJobIdFunction(
//      []() -> std::string { return "different_job_id"; });
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_TRUE(cmdResponse.SendResponse);
//  EXPECT_TRUE(isErrorResponse(cmdResponse));
//}
//
// TEST_F(StopHandlerTest,
//       validateStopCommandWithMismatchingJobIDAndMismatchingServiceID) {
//  _stopMessage.ServiceID = "another_service_id";
//  _handlerUnderTest->registerGetJobIdFunction(
//      []() -> std::string { return "different_job_id"; });
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_FALSE(cmdResponse.SendResponse);
//  EXPECT_FALSE(
//      isErrorResponse(cmdResponse)); // mismatching service ids get tested
//      first
//}
//
// TEST_F(StopHandlerTest, validateStopCommandWithInvalidCommandID) {
//  _stopMessage.CommandID = "invalid";
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_TRUE(cmdResponse.SendResponse);
//  EXPECT_TRUE(isErrorResponse(cmdResponse));
//}
//
// TEST_F(StopHandlerTest, validateStopCommandImmediateStop) {
//  _stopMessage.StopTime = time_point{0ms};
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_TRUE(cmdResponse.SendResponse);
//  EXPECT_FALSE(isErrorResponse(cmdResponse));
//  EXPECT_EQ(cmdResponse.StatusCode, 201);
//}
//
// TEST_F(StopHandlerTest, validateStopCommandSetStopTime) {
//  _stopMessage.StopTime =
//      std::chrono::system_clock::now() + std::chrono::minutes(5);
//
//  CmdResponse cmdResponse = _handlerUnderTest->stopWriting(_stopMessage);
//
//  EXPECT_TRUE(cmdResponse.SendResponse);
//  EXPECT_FALSE(isErrorResponse(cmdResponse));
//  EXPECT_EQ(cmdResponse.StatusCode, 201);
//}
