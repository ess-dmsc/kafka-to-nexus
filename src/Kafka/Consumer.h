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
#include <chrono>
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
  virtual void addTopic(std::string const &Topic) = 0;
  virtual std::pair<PollStatus, FileWriter::Msg> poll() = 0;
  virtual std::vector<int32_t>
  queryTopicPartitions(const std::string &TopicName) = 0;
  virtual void addPartitionAtOffset(std::string const &Topic, int PartitionId,
                                    int64_t Offset) = 0;
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
  /// Adds topic to consumer at latest offset.
  ///
  /// \param Topic The name of the topic to add.
  void addTopic(std::string const &Topic) override;

  /// Set a topic partition at a specified offset to consume from.
  ///
  /// Replaces any exisiting topics + partitions that are currently being
  /// consumed.
  /// \note This is a non blocking call.
  void addPartitionAtOffset(std::string const &Topic, int PartitionId,
                            int64_t Offset) override;

  /// Get a list of partition numbers in a topic.
  ///
  /// \param Topic The name of the topic to query.
  /// \return List of partition numbers on topic.
  std::vector<int32_t> queryTopicPartitions(const std::string &Topic) override;
  /// Polls for any new messages.
  ///
  /// \return Any new messages consumed.
  std::pair<PollStatus, FileWriter::Msg> poll() override;

protected:
  std::unique_ptr<RdKafka::KafkaConsumer> KafkaConsumer;

private:
  std::unique_ptr<RdKafka::Conf> Conf;
  BrokerSettings ConsumerBrokerSettings;
  std::unique_ptr<RdKafka::Metadata> getMetadata();
  int id = 0;
  std::unique_ptr<KafkaEventCb> EventCallback;
  void assignToPartitions(
      const std::string &Topic,
      const std::vector<RdKafka::TopicPartition *> &TopicPartitionsWithOffsets);
  std::vector<RdKafka::TopicPartition *>
  queryWatermarkOffsets(const std::string &Topic);
  std::unique_ptr<RdKafka::Metadata> metadataCall();
  SharedLogger Logger = spdlog::get("filewriterlogger");
};

class StubConsumer : public ConsumerInterface {
public:
  ~StubConsumer() override = default;

  void addTopic(std::string const &Topic) override { UNUSED_ARG(Topic); };

  std::pair<PollStatus, FileWriter::Msg> poll() override {
    return {PollStatus::TimedOut, FileWriter::Msg()};
  };

  std::vector<int32_t>
  queryTopicPartitions(const std::string &TopicName) override {
    UNUSED_ARG(TopicName);
    return {};
  };

  void addPartitionAtOffset(std::string const &Topic, int PartitionId,
                            int64_t Offset) override {
    UNUSED_ARG(Topic);
    UNUSED_ARG(PartitionId);
    UNUSED_ARG(Offset);
  };
};
} // namespace Kafka
