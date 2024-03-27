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

class StubConsumer : public ConsumerInterface {
public:
  ~StubConsumer() override = default;

  std::pair<PollStatus, FileWriter::Msg> poll() override {
    return {PollStatus::TimedOut, FileWriter::Msg()};
  };

  void addPartitionAtOffset([[maybe_unused]] std::string const &Topic, [[maybe_unused]] int PartitionId,
                            [[maybe_unused]] int64_t Offset) override {
  };

  void addTopic([[maybe_unused]] std::string const &Topic) override {}

  void assignAllPartitions([[maybe_unused]] std::string const &Topic,
                           [[maybe_unused]] time_point const &StartTimestamp) override {
  }

  const RdKafka::TopicMetadata *
  getTopicMetadata([[maybe_unused]] const std::string &Topic,
                   [[maybe_unused]] RdKafka::Metadata *MetadataPtr) override {
    return nullptr;
  }
};
} // namespace Kafka
