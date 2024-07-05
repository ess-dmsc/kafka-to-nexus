// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandSystem/FeedbackProducer.h"
#include <answ_action_response_generated.h>
#include <gtest/gtest.h>
#include <wrdn_finished_writing_generated.h>

using std::chrono_literals::operator""ms;

TEST(FeedbackProducer, publish_start_job_success) {
  auto producer_topic = std::make_unique<Kafka::StubProducerTopic>("my_topic");
  auto holder = producer_topic.get();
  auto feedback_producer = Command::FeedbackProducer::create_null(
      "my_service_identifier", std::move(producer_topic));

  feedback_producer->publishResponse(
      Command::ActionResponse::StartJob, Command::ActionResult::Success,
      "my_job_id", "my_command_id", time_point{123456ms}, 123, "my_message");

  auto message = holder->messages.at(0).get();
  auto result = GetActionResponse(message->data);
  ASSERT_EQ(result->service_id()->str(), "my_service_identifier");
  ASSERT_EQ(result->job_id()->str(), "my_job_id");
  ASSERT_EQ(result->action(), ActionType::StartJob);
  ASSERT_EQ(result->outcome(), ActionOutcome::Success);
  ASSERT_EQ(result->status_code(), 123);
  ASSERT_EQ(result->stop_time(), uint64_t{123456});
  ASSERT_EQ(result->message()->str(), "my_message");
  ASSERT_EQ(result->command_id()->str(), "my_command_id");
}

TEST(FeedbackProducer, publish_start_job_failure) {
  auto producer_topic = std::make_unique<Kafka::StubProducerTopic>("my_topic");
  auto holder = producer_topic.get();
  auto feedback_producer = Command::FeedbackProducer::create_null(
      "my_service_identifier", std::move(producer_topic));

  feedback_producer->publishResponse(
      Command::ActionResponse::StartJob, Command::ActionResult::Failure,
      "my_job_id", "my_command_id", time_point{123456ms}, 123, "my_message");

  auto message = holder->messages.at(0).get();
  auto result = GetActionResponse(message->data);
  ASSERT_EQ(result->action(), ActionType::StartJob);
  ASSERT_EQ(result->outcome(), ActionOutcome::Failure);
}

TEST(FeedbackProducer, publish_stop_writing_success) {
  auto producer_topic = std::make_unique<Kafka::StubProducerTopic>("my_topic");
  auto holder = producer_topic.get();
  auto feedback_producer = Command::FeedbackProducer::create_null(
      "my_service_identifier", std::move(producer_topic));

  feedback_producer->publishStoppedMsg(Command::ActionResult::Success,
                                       "my_job_id", "my_message",
                                       {"c://file.txt"}, "my_metadata");

  auto message = holder->messages.at(0).get();
  auto result = GetFinishedWriting(message->data);
  ASSERT_EQ(result->service_id()->str(), "my_service_identifier");
  ASSERT_EQ(result->job_id()->str(), "my_job_id");
  ASSERT_EQ(result->error_encountered(), false);
  ASSERT_EQ(result->file_name()->str(), "c://file.txt");
  ASSERT_EQ(result->metadata()->str(), "my_metadata");
  ASSERT_EQ(result->message()->str(), "my_message");
}

TEST(FeedbackProducer, publish_stop_writing_failure) {
  auto producer_topic = std::make_unique<Kafka::StubProducerTopic>("my_topic");
  auto holder = producer_topic.get();
  auto feedback_producer = Command::FeedbackProducer::create_null(
      "my_service_identifier", std::move(producer_topic));

  feedback_producer->publishStoppedMsg(Command::ActionResult::Failure,
                                       "my_job_id", "my_message",
                                       {"c://file.txt"}, "my_metadata");

  auto message = holder->messages.at(0).get();
  auto result = GetFinishedWriting(message->data);
  ASSERT_EQ(result->error_encountered(), true);
}
