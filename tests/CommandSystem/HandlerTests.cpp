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

class StartHandlerTest : public ::testing::Test {
protected:
  std::shared_ptr<Kafka::StubConsumerFactory> _consumer_factory;
  std::unique_ptr<Handler> _handlerUnderTest;
  Kafka::StubProducerTopic *_producer_topic;
  std::string const _valid_job_id = "123e4567-e89b-12d3-a456-426614174000";
  std::string const _serviceId = "service_id_123";
  std::string const _default_command_topic = "command_topic";

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
  }

  void queue_start_message(std::string const &command_topic,
                           std::string const &job_id,
                           std::optional<std::string> const &service_id) {
    auto start_message = RunStartStopHelpers::buildRunStartMessage(
        "my_instrument", "my_run_name", example_json, job_id, service_id,
        "my_file_name", 123456, 987654, command_topic);
    FileWriter::MessageMetaData meta_data{.topic = "pool_topic"};
    _consumer_factory->messages->emplace_back(start_message.data(),
                                              start_message.size(), meta_data);
  }
};

TEST_F(StartHandlerTest, start_command_actioned) {
  bool start_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      [&start_called]([[maybe_unused]] auto startMessage) -> void {
        start_called = true;
      });
  queue_start_message("new_command_topic", _valid_job_id, _serviceId);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(true, start_called);
}

TEST_F(StartHandlerTest, start_command_ignored_if_already_writing) {
  bool start_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });
  _handlerUnderTest->registerStartFunction(
      [&start_called]([[maybe_unused]] auto startMessage) -> void {
        start_called = true;
      });
  queue_start_message("new_command_topic", _valid_job_id, _serviceId);
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
  queue_start_message("new_command_topic", _valid_job_id, {});

  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(true, start_called);
}

TEST_F(StartHandlerTest,
       start_command_with_invalid_uuid_for_job_id_is_rejected) {
  bool start_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      [&start_called]([[maybe_unused]] auto startMessage) -> void {
        start_called = true;
      });
  queue_start_message("new_command_topic", "invalid_uuid", {_serviceId});
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(false, start_called);
}

TEST_F(StartHandlerTest, start_command_is_echoed) {
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      []([[maybe_unused]] auto startMessage) -> void {});

  // NOTE: using the same command topic, so it doesn't create a new
  // ProducerTopic. This allows us to see the messages
  queue_start_message(_default_command_topic, _valid_job_id, _serviceId);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  auto message = _producer_topic->messages.at(0).get();
  std::string schema{message->v.begin() + 4, message->v.begin() + 8};

  ASSERT_EQ(schema, "pl72");
}

TEST_F(StartHandlerTest, start_command_sends_response) {
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      []([[maybe_unused]] auto startMessage) -> void {});

  // NOTE: using the same command topic, so it doesn't create a new
  // ProducerTopic. This allows us to see the messages
  queue_start_message(_default_command_topic, _valid_job_id, _serviceId);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  auto message = _producer_topic->messages.at(1).get();
  auto result = GetActionResponse(message->data());

  // Expect 2 messages: the echo of the start command and the response
  EXPECT_EQ(2u, _producer_topic->messages.size());
  ASSERT_EQ(result->action(), ActionType::StartJob);
  ASSERT_EQ(result->outcome(), ActionOutcome::Success);
}

TEST_F(StartHandlerTest, rejected_command_sends_response) {
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStartFunction(
      []([[maybe_unused]] auto startMessage) -> void {});

  // NOTE: using the same command topic, so it doesn't create a new
  // ProducerTopic. This allows us to see the messages
  queue_start_message(_default_command_topic, "invalid_uuid", {_serviceId});
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  auto message = _producer_topic->messages.at(1).get();
  auto result = GetActionResponse(message->data());

  // Expect 2 messages: the echo of the start command and the response
  EXPECT_EQ(2u, _producer_topic->messages.size());
  ASSERT_EQ(result->action(), ActionType::StartJob);
  ASSERT_EQ(result->outcome(), ActionOutcome::Failure);
}

class StopHandlerTest : public ::testing::Test {
protected:
  std::shared_ptr<Kafka::StubConsumerFactory> _consumer_factory;
  std::unique_ptr<Handler> _handlerUnderTest;
  Kafka::StubProducerTopic *_producer_topic;
  std::string const _valid_job_id = "123e4567-e89b-12d3-a456-426614174000";
  std::string const _valid_command_id = "dda7c081-737d-42b5-996c-f45a4a957536";
  std::string const _serviceId = "service_id_123";
  std::string const _default_command_topic = "command_topic";

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
  }

  void queue_stop_message(uint64_t stop_time, std::string const &job_id,
                          std::optional<std::string> const &service_id,
                          std::string const &command_id) {
    auto stop_message = RunStartStopHelpers::buildRunStopMessage(
        stop_time, "my_run_name", job_id, command_id, service_id);
    FileWriter::MessageMetaData meta_data{.topic = _default_command_topic};
    _consumer_factory->messages->emplace_back(stop_message.data(),
                                              stop_message.size(), meta_data);
  }
};

TEST_F(StopHandlerTest, stop_now_command_actioned) {
  bool stop_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });
  _handlerUnderTest->registerStopNowFunction(
      [&stop_called]() -> void { stop_called = true; });
  _handlerUnderTest->registerGetJobIdFunction(
      [this]() -> std::string { return this->_valid_job_id; });
  queue_stop_message(0, _valid_job_id, {_serviceId}, _valid_command_id);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(true, stop_called);
}

TEST_F(StopHandlerTest, stop_ignored_if_not_writing) {
  bool stop_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return false; });
  _handlerUnderTest->registerStopNowFunction(
      [&stop_called]() -> void { stop_called = true; });
  _handlerUnderTest->registerGetJobIdFunction(
      [this]() -> std::string { return this->_valid_job_id; });
  queue_stop_message(0, _valid_job_id, {_serviceId}, _valid_command_id);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(false, stop_called);
}

TEST_F(StopHandlerTest, stop_ignored_if_job_id_wrong) {
  bool stop_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });
  _handlerUnderTest->registerStopNowFunction(
      [&stop_called]() -> void { stop_called = true; });
  _handlerUnderTest->registerGetJobIdFunction(
      [this]() -> std::string { return this->_valid_job_id; });
  std::string wrong_job_id = "43e86e91-75dd-4c87-996b-fd7a22d0e726";
  queue_stop_message(0, wrong_job_id, {_serviceId}, _valid_command_id);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(false, stop_called);
}

TEST_F(StopHandlerTest, stop_ignored_if_no_job_id) {
  bool stop_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });
  _handlerUnderTest->registerStopNowFunction(
      [&stop_called]() -> void { stop_called = true; });
  _handlerUnderTest->registerGetJobIdFunction(
      [this]() -> std::string { return this->_valid_job_id; });
  queue_stop_message(0, "", {_serviceId}, _valid_command_id);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(false, stop_called);
}

TEST_F(StopHandlerTest, stop_actioned_if_service_id_in_message_is_empty) {
  bool stop_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });
  _handlerUnderTest->registerStopNowFunction(
      [&stop_called]() -> void { stop_called = true; });
  _handlerUnderTest->registerGetJobIdFunction(
      [this]() -> std::string { return this->_valid_job_id; });
  queue_stop_message(0, _valid_job_id, {}, _valid_command_id);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(true, stop_called);
}

TEST_F(StopHandlerTest, stop_ignored_if_command_id_is_not_uuid) {
  bool stop_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });
  _handlerUnderTest->registerStopNowFunction(
      [&stop_called]() -> void { stop_called = true; });
  _handlerUnderTest->registerGetJobIdFunction(
      [this]() -> std::string { return this->_valid_job_id; });
  queue_stop_message(0, _valid_job_id, {_serviceId}, "not_a_uuid");
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  EXPECT_EQ(false, stop_called);
}

TEST_F(StopHandlerTest, stop_command_sends_response) {
  bool stop_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });
  _handlerUnderTest->registerStopNowFunction(
      [&stop_called]() -> void { stop_called = true; });
  _handlerUnderTest->registerGetJobIdFunction(
      [this]() -> std::string { return this->_valid_job_id; });
  queue_stop_message(0, _valid_job_id, {_serviceId}, _valid_command_id);
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  auto message = _producer_topic->messages.at(0).get();
  auto result = GetActionResponse(message->data());

  EXPECT_EQ(1u, _producer_topic->messages.size());
  ASSERT_EQ(result->action(), ActionType::SetStopTime);
  ASSERT_EQ(result->outcome(), ActionOutcome::Success);
}

TEST_F(StopHandlerTest, rejected_stop_command_sends_response) {
  bool stop_called = false;
  _handlerUnderTest->registerIsWritingFunction([]() -> bool { return true; });
  _handlerUnderTest->registerStopNowFunction(
      [&stop_called]() -> void { stop_called = true; });
  _handlerUnderTest->registerGetJobIdFunction(
      [this]() -> std::string { return this->_valid_job_id; });
  queue_stop_message(0, _valid_job_id, {_serviceId}, "invalid_command_id");
  // First poll connects the consumer
  _handlerUnderTest->loopFunction();

  _handlerUnderTest->loopFunction();

  auto message = _producer_topic->messages.at(0).get();
  auto result = GetActionResponse(message->data());

  EXPECT_EQ(1u, _producer_topic->messages.size());
  ASSERT_EQ(result->action(), ActionType::SetStopTime);
  ASSERT_EQ(result->outcome(), ActionOutcome::Failure);
}
