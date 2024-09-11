// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatBufferGenerators.h"
#include "FlatbufferReader.h"
#include "Metrics/Registrar.h"
#include "Stream/MessageWriter.h"
#include "Stream/Partition.h"
#include "TimeUtility.h"
#include "WriterModuleBase.h"
#include "helpers/SetExtractorModule.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>

using std::chrono_literals::operator""s;

class FakePartitionFilter : public Stream::IPartitionFilter {
public:
  FakePartitionFilter() : Stream::IPartitionFilter() {}

  void setStopTime(time_point stop) override { stop_time = stop; }

  [[nodiscard]] bool shouldStopPartition(
      [[maybe_unused]] Kafka::PollStatus current_poll_status) override {
    return should_stop;
  }

  [[nodiscard]] PartitionState currentPartitionState() const override {
    return state;
  }

  [[nodiscard]] bool hasErrorState() const override {
    return state == PartitionState::ERROR;
  }

  [[nodiscard]] bool hasTopicTimedOut() const override { return has_timed_out; }

  [[nodiscard]] time_point getStatusOccurrenceTime() const override {
    return status_occurrence_time;
  }

  bool has_timed_out{false};
  time_point stop_time{time_point::max()};
  PartitionState state{PartitionState::DEFAULT};
  bool should_stop{false};
  Kafka::PollStatus poll_status{Kafka::PollStatus::Message};
  time_point status_occurrence_time{time_point::min()};
};

class FakeSourceFilter : public Stream::ISourceFilter {
public:
  FakeSourceFilter() : Stream::ISourceFilter() {}

  bool filter_message(FileWriter::FlatbufferMessage const &message) override {
    last_message = message;
    return true;
  }

  void set_stop_time(time_point new_stop_time) override {
    stop_time = new_stop_time;
  }

  bool has_finished() const override { return has_finished_processing; }

  void set_source_hash(
      FileWriter::FlatbufferMessage::SrcHash new_source_hash) override {
    source_hash = new_source_hash;
  }

  FileWriter::FlatbufferMessage last_message;
  time_point stop_time{time_point::max()};
  bool has_finished_processing{false};
  FileWriter::FlatbufferMessage::SrcHash source_hash;
};

TEST(partition_test, is_not_finished_if_source_filter_says_do_not_stop) {
  auto messages = std::make_shared<std::vector<FileWriter::Msg>>();
  auto stub_consumer = std::make_shared<Kafka::StubConsumer>(messages);
  auto registrar = std::make_unique<Metrics::Registrar>("some_prefix");
  time_point Stop{100s};
  duration StopLeeway{5s};
  std::function<bool()> AreStreamersPausedFunction = []() { return false; };

  std::unique_ptr<Stream::IPartitionFilter> partition_filter =
      std::make_unique<FakePartitionFilter>();
  auto partition_filter_ref =
      dynamic_cast<FakePartitionFilter *>(partition_filter.get());
  partition_filter_ref->should_stop = false;

  auto partition = Stream::Partition(
      stub_consumer, 1, "topic_name", {}, std::move(partition_filter),
      registrar.get(), Stop, StopLeeway, AreStreamersPausedFunction);

  partition.pollForMessage();
  partition.pollForMessage();
  partition.pollForMessage();

  EXPECT_EQ(partition.hasFinished(), false);
}

TEST(partition_test, is_finished_if_source_filter_says_stop) {
  auto messages = std::make_shared<std::vector<FileWriter::Msg>>();
  auto stub_consumer = std::make_shared<Kafka::StubConsumer>(messages);
  auto registrar = std::make_unique<Metrics::Registrar>("some_prefix");
  time_point Stop{100s};
  duration StopLeeway{5s};
  std::function<bool()> AreStreamersPausedFunction = []() { return false; };

  std::unique_ptr<Stream::IPartitionFilter> partition_filter =
      std::make_unique<FakePartitionFilter>();
  auto partition_filter_ref =
      dynamic_cast<FakePartitionFilter *>(partition_filter.get());
  partition_filter_ref->should_stop = true;

  auto partition = Stream::Partition(
      stub_consumer, 1, "topic_name", {}, std::move(partition_filter),
      registrar.get(), Stop, StopLeeway, AreStreamersPausedFunction);

  partition.pollForMessage();
  partition.pollForMessage();
  partition.pollForMessage();

  EXPECT_EQ(partition.hasFinished(), true);
}

TEST(partition_test, setting_stop_time_updates_source_filter) {
  auto messages = std::make_shared<std::vector<FileWriter::Msg>>();
  auto stub_consumer = std::make_shared<Kafka::StubConsumer>(messages);
  auto registrar = std::make_unique<Metrics::Registrar>("some_prefix");
  time_point Stop{100s};
  duration StopLeeway{5s};
  std::function<bool()> AreStreamersPausedFunction = []() { return false; };

  std::unique_ptr<Stream::IPartitionFilter> partition_filter =
      std::make_unique<FakePartitionFilter>();
  auto partition_filter_ref =
      dynamic_cast<FakePartitionFilter *>(partition_filter.get());

  auto partition = Stream::Partition(
      stub_consumer, 1, "topic_name", {}, std::move(partition_filter),
      registrar.get(), Stop, StopLeeway, AreStreamersPausedFunction);

  partition.setStopTime(time_point{123s});

  EXPECT_EQ(partition_filter_ref->stop_time, time_point{123s});
}

TEST(partition_test, immediate_stop) {
  auto messages = std::make_shared<std::vector<FileWriter::Msg>>();
  auto stub_consumer = std::make_shared<Kafka::StubConsumer>(messages);
  auto registrar = std::make_unique<Metrics::Registrar>("some_prefix");
  time_point Stop{100s};
  duration StopLeeway{5s};
  std::function<bool()> AreStreamersPausedFunction = []() { return false; };

  std::unique_ptr<Stream::IPartitionFilter> partition_filter =
      std::make_unique<FakePartitionFilter>();

  auto partition = Stream::Partition(
      stub_consumer, 1, "topic_name", {}, std::move(partition_filter),
      registrar.get(), Stop, StopLeeway, AreStreamersPausedFunction);

  partition.stop();
  partition.pollForMessage();

  EXPECT_EQ(partition.hasFinished(), true);
}

TEST(partition_test, if_initial_stop_time_too_close_to_max_then_is_backed_off) {
  auto messages = std::make_shared<std::vector<FileWriter::Msg>>();
  auto stub_consumer = std::make_shared<Kafka::StubConsumer>(messages);
  auto registrar = std::make_unique<Metrics::Registrar>("some_prefix");
  time_point Stop{time_point::max()};
  duration StopLeeway{5s};
  std::function<bool()> AreStreamersPausedFunction = []() { return false; };

  std::unique_ptr<Stream::IPartitionFilter> partition_filter =
      std::make_unique<FakePartitionFilter>();
  auto partition_filter_ref =
      dynamic_cast<FakePartitionFilter *>(partition_filter.get());

  auto partition = Stream::Partition(
      stub_consumer, 1, "topic_name", {}, std::move(partition_filter),
      registrar.get(), Stop, StopLeeway, AreStreamersPausedFunction);

  EXPECT_EQ(partition_filter_ref->stop_time, Stop - StopLeeway);
}

TEST(partition_test, if_new_stop_time_too_close_to_max_then_is_backed_off) {
  auto messages = std::make_shared<std::vector<FileWriter::Msg>>();
  auto stub_consumer = std::make_shared<Kafka::StubConsumer>(messages);
  auto registrar = std::make_unique<Metrics::Registrar>("some_prefix");
  time_point Stop{100s};
  duration StopLeeway{5s};
  std::function<bool()> AreStreamersPausedFunction = []() { return false; };

  std::unique_ptr<Stream::IPartitionFilter> partition_filter =
      std::make_unique<FakePartitionFilter>();
  auto partition_filter_ref =
      dynamic_cast<FakePartitionFilter *>(partition_filter.get());

  auto partition = Stream::Partition(
      stub_consumer, 1, "topic_name", {}, std::move(partition_filter),
      registrar.get(), Stop, StopLeeway, AreStreamersPausedFunction);

  auto new_stop_time{time_point::max()};
  partition.setStopTime(new_stop_time);

  EXPECT_EQ(partition_filter_ref->stop_time, new_stop_time - StopLeeway);
}

TEST(partition_test, sends_messages_to_source_filters) {
  auto messages = std::make_shared<std::vector<FileWriter::Msg>>();
  auto stub_consumer = std::make_shared<Kafka::StubConsumer>(messages);
  stub_consumer->addTopic("topic_name");
  auto registrar = std::make_unique<Metrics::Registrar>("some_prefix");
  time_point Stop{100s};
  duration StopLeeway{5s};
  std::function<bool()> AreStreamersPausedFunction = []() { return false; };
  std::unique_ptr<Stream::IPartitionFilter> partition_filter =
      std::make_unique<FakePartitionFilter>();
  auto source_filter_1 = std::make_unique<FakeSourceFilter>();
  auto source_filter_1_ptr = source_filter_1.get();
  auto source_filter_2 = std::make_unique<FakeSourceFilter>();
  auto source_filter_2_ptr = source_filter_1.get();
  std::vector<std::unique_ptr<Stream::ISourceFilter>> source_filters;
  source_filters.emplace_back(std::move(source_filter_1));
  source_filters.emplace_back(std::move(source_filter_2));
  auto partition = Stream::Partition(
      stub_consumer, 1, "topic_name", std::move(source_filters),
      std::move(partition_filter), registrar.get(), Stop, StopLeeway,
      AreStreamersPausedFunction);
  auto const [buffer, size] =
      FlatBuffers::create_f144_message_double("delay:source:chopper", 100, 123);
  FileWriter::MessageMetaData metadata;
  metadata.Timestamp = 123ms;
  metadata.Offset = 0;
  metadata.Partition = 1;
  metadata.topic = "topic_name";
  FileWriter::Msg msg1{buffer.get(), size, metadata};
  messages->emplace_back(std::move(msg1));

  partition.pollForMessage();

  EXPECT_EQ(source_filter_1_ptr->last_message.getSourceName(),
            "delay:source:chopper");
  EXPECT_EQ(source_filter_1_ptr->last_message.getTimestamp(), 123000000);
  EXPECT_EQ(source_filter_2_ptr->last_message.getSourceName(),
            "delay:source:chopper");
  EXPECT_EQ(source_filter_2_ptr->last_message.getTimestamp(), 123000000);
}

TEST(partition_test, sends_stop_time_to_source_filters) {
  auto messages = std::make_shared<std::vector<FileWriter::Msg>>();
  auto stub_consumer = std::make_shared<Kafka::StubConsumer>(messages);
  auto registrar = std::make_unique<Metrics::Registrar>("some_prefix");
  time_point Stop{100s};
  duration StopLeeway{5s};
  std::function<bool()> AreStreamersPausedFunction = []() { return false; };
  std::unique_ptr<Stream::IPartitionFilter> partition_filter =
      std::make_unique<FakePartitionFilter>();
  auto source_filter_1 = std::make_unique<FakeSourceFilter>();
  auto source_filter_1_ptr = source_filter_1.get();
  auto source_filter_2 = std::make_unique<FakeSourceFilter>();
  auto source_filter_2_ptr = source_filter_1.get();
  std::vector<std::unique_ptr<Stream::ISourceFilter>> source_filters;
  source_filters.emplace_back(std::move(source_filter_1));
  source_filters.emplace_back(std::move(source_filter_2));
  auto partition = Stream::Partition(
      stub_consumer, 1, "topic_name", std::move(source_filters),
      std::move(partition_filter), registrar.get(), Stop, StopLeeway,
      AreStreamersPausedFunction);

  partition.setStopTime(time_point{123ms});

  EXPECT_EQ(source_filter_1_ptr->stop_time, time_point{123ms});
  EXPECT_EQ(source_filter_2_ptr->stop_time, time_point{123ms});
}
