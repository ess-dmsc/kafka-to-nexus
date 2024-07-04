// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "BrokerSettings.h"
#include "ConsumerRebalanceCb.h"
#include "KafkaEventCb.h"
#include "Msg.h"
#include "PollStatus.h"
#include "TimeUtility.h"
#include <librdkafka/rdkafkacpp.h>
#include <memory>

namespace FileWriter {
struct Msg;
}

namespace Kafka {

class ConsumerInterface {
public:
  ConsumerInterface() = default;
  virtual ~ConsumerInterface() = default;
  virtual std::pair<PollStatus, FileWriter::Msg> poll() = 0;
  virtual void addPartitionAtOffset(std::string const &Topic, int PartitionId,
                                    int64_t Offset) = 0;
  virtual void addTopic(std::string const &Topic) = 0;
  virtual void assignAllPartitions(std::string const &Topic,
                                   time_point const &StartTimestamp) = 0;
  virtual const RdKafka::TopicMetadata *
  getTopicMetadata(const std::string &Topic,
                   RdKafka::Metadata *MetadataPtr) = 0;
};

class Consumer : public ConsumerInterface {
public:
  /// The constructor
  ///
  /// \param RdConsumer The RdKafka Consumer to wrap.
  /// \param RdConf The Configuration for the RdKafka Consumer.
  /// \param EventCb Callback for each time the consumer is polled.
  Consumer(std::unique_ptr<RdKafka::KafkaConsumer> RdConsumer,
           std::unique_ptr<RdKafka::Conf> RdConf,
           std::unique_ptr<KafkaEventCb> EventCb);
  Consumer(Consumer &&) = delete;
  Consumer(Consumer const &) = delete;
  ~Consumer() override;

  /// Add a topic partition at a specified offset to consume from.
  ///
  /// Previously assigned topic partitions are preserved.
  /// \note This is a non blocking call.
  void addPartitionAtOffset(std::string const &Topic, int PartitionId,
                            int64_t Offset) override;

  void addTopic(std::string const &Topic) override;

  /// Assign all topic's partitions using the offsets defined by the
  /// provided timestamp.
  ///
  /// Previous partition assignments are NOT preserved.
  void assignAllPartitions(std::string const &Topic,
                           time_point const &StartTimestamp) override;

  /// \brief Polls for any new messages.
  /// \note Is a blocking call with a timeout that is hard coded in the broker
  /// settings. \return Any new messages consumed.
  std::pair<PollStatus, FileWriter::Msg> poll() override;

  /// Obtain metadata for given topic.
  /// \param Topic Topic. \param MetadataPtr Pointer to store the metadata.
  const RdKafka::TopicMetadata *
  getTopicMetadata(const std::string &Topic,
                   RdKafka::Metadata *MetadataPtr) override;

private:
  std::unique_ptr<RdKafka::Conf> Conf;
  BrokerSettings const ConsumerBrokerSettings;
  int id = 0;
  std::unique_ptr<KafkaEventCb> EventCallback;
  std::unique_ptr<RdKafka::KafkaConsumer> KafkaConsumer;
};

class StubConsumer : public Kafka::ConsumerInterface {
public:
  // TODO: pass valid topics, so we can throw on addPartition, addTopic, etc.
  explicit StubConsumer(std::shared_ptr<std::vector<FileWriter::Msg>> messages,
                        const std::vector<std::string> &valid_topics = {})
      : _messages(std::move(messages)), _valid_topics(valid_topics) {}
  ~StubConsumer() override = default;

  std::pair<Kafka::PollStatus, FileWriter::Msg> poll() override {
    // Look for next message with the correct topic
    while (_offset < _messages->size()) {
      auto const message_topic = _messages->at(_offset).getMetaData().topic;
      if (message_topic == _topic) {
        auto temp = FileWriter::Msg{_messages->at(_offset).data(),
                                    _messages->at(_offset).size(),
                                    _messages->at(_offset).getMetaData()};
        ++_offset;
        return {Kafka::PollStatus::Message, std::move(temp)};
      }
      ++_offset;
    }

    // No more messages
    if (_at_end_of_partition) {
      return {Kafka::PollStatus::TimedOut, FileWriter::Msg()};
    } else {
      _at_end_of_partition = true;
      return {Kafka::PollStatus::EndOfPartition, FileWriter::Msg()};
    }
  };

  void addPartitionAtOffset(std::string const &Topic, int PartitionId,
                            [[maybe_unused]] int64_t Offset) override {
    if (!is_topic_valid(Topic)) {
      throw std::runtime_error("Could not add partition at offset");
    }
    _topic = Topic;
    _partition = PartitionId;
  };

  void addTopic(std::string const &Topic) override {
    if (!is_topic_valid(Topic)) {
      throw std::runtime_error("Could not add topic");
    }
    _topic = Topic;
  }

  void assignAllPartitions(
      std::string const &Topic,
      [[maybe_unused]] time_point const &StartTimestamp) override {
    if (!is_topic_valid(Topic)) {
      throw std::runtime_error("Could not assign all partitions");
    }
    _topic = Topic;
  }

  const RdKafka::TopicMetadata *
  getTopicMetadata([[maybe_unused]] const std::string &Topic,
                   [[maybe_unused]] RdKafka::Metadata *MetadataPtr) override {
    return nullptr;
  }

private:
  bool is_topic_valid(std::string const &topic) {
    if (_valid_topics.empty()) {
      return true;
    }

    return std::find(_valid_topics.begin(), _valid_topics.end(), topic) !=
           _valid_topics.end();
  }

  size_t _offset = 0;
  std::string _topic;
  int32_t _partition = 0;
  std::shared_ptr<std::vector<FileWriter::Msg>> _messages;
  bool _at_end_of_partition = false;
  std::vector<std::string> _valid_topics;
};
} // namespace Kafka
