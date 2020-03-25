// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "KafkaW/Consumer.h"
#include "KafkaW/MetadataException.h"
#include "Metrics/Registrar.h"
#include "Stream/Partition.h"
#include "Stream/Topic.h"
#include <cstdint>
#include <future>
#include <gtest/gtest.h>
#include <memory>
#include <trompeloeil.hpp>
#include <utility>
#include <vector>

using std::chrono_literals::operator""s;

class TopicStandIn : public Stream::Topic {
public:
  TopicStandIn(KafkaW::BrokerSettings Settings, std::string const &Topic,
               Stream::SrcToDst Map, Stream::MessageWriter *Writer,
               Metrics::Registrar &RegisterMetric, Stream::time_point StartTime,
               Stream::duration StartTimeLeeway, Stream::time_point StopTime,
               Stream::duration StopTimeLeeway)
      : Stream::Topic(Settings, Topic, Map, Writer, RegisterMetric, StartTime,
                      StartTimeLeeway, StopTime, StopTimeLeeway) {}
  using Topic::ConsumerThreads;
  using Topic::CurrentMetadataTimeOut;
  using Topic::Executor;
  using offset_list = std::vector<std::pair<int, int64_t>>;
  MAKE_CONST_MOCK5(getOffsetForTimeInternal,
                   offset_list(std::string, std::string, std::vector<int>,
                               Stream::time_point, Stream::duration),
                   override);
  MAKE_CONST_MOCK3(getPartitionsForTopicInternal,
                   std::vector<int>(std::string, std::string, Stream::duration),
                   override);
  MAKE_MOCK2(getPartitionsForTopic, void(KafkaW::BrokerSettings, std::string),
             override);
  MAKE_MOCK3(getOffsetsForPartitions,
             void(KafkaW::BrokerSettings, std::string, std::vector<int>),
             override);
  MAKE_MOCK3(createStreams,
             void(KafkaW::BrokerSettings, std::string,
                  std::vector<std::pair<int, int64_t>>),
             override);
  void initMetadataCalls(KafkaW::BrokerSettings, std::string) override {}
  void initMetadataCallsBase(KafkaW::BrokerSettings Settings,
                             std::string Topic) {
    Topic::initMetadataCalls(Settings, Topic);
  }

  void getPartitionsForTopicBase(KafkaW::BrokerSettings Settings,
                                 std::string Topic) {
    Topic::getPartitionsForTopic(Settings, Topic);
  }

  void getOffsetsForPartitionsBase(KafkaW::BrokerSettings Settings,
                                   std::string Topic,
                                   std::vector<int> Partitions) {
    Topic::getOffsetsForPartitions(Settings, Topic, Partitions);
  }

  void
  createStreamsBase(KafkaW::BrokerSettings Settings, std::string Topic,
                    std::vector<std::pair<int, int64_t>> PartitionOffsets) {
    Topic::createStreams(Settings, Topic, PartitionOffsets);
  }
};

class TopicTest : public ::testing::Test {
public:
  auto createTestedInstance() {
    return std::make_unique<TopicStandIn>(KafkaSettings, UsedTopicName, Map,
                                          nullptr, Registrar, Start, 5s, Stop,
                                          5s);
  }
  std::string const UsedTopicName{"some_topic_or_another"};
  KafkaW::BrokerSettings KafkaSettings;
  Stream::time_point Start{std::chrono::system_clock::now()};
  Stream::time_point Stop{std::chrono::system_clock::time_point::max()};
  Metrics::Registrar Registrar{"some_name", {}};
  Stream::SrcToDst Map;
};

using std::chrono_literals::operator""ms;
using trompeloeil::_;

TEST_F(TopicTest, StartMetaDataCall) {
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*UnderTest, getPartitionsForTopic(_, UsedTopicName)).TIMES(1);
  UnderTest->initMetadataCallsBase(KafkaSettings, UsedTopicName);

  // Wait until we are done processing "getPartitionsForTopic
  std::promise<bool> Promise;
  auto Future = Promise.get_future();
  UnderTest->Executor.sendLowPriorityWork(
      [&Promise]() { Promise.set_value(true); });
  Future.wait();
}

TEST_F(TopicTest, GetPartitionsForTopicException) {
  auto UnderTest = createTestedInstance();

  REQUIRE_CALL(*UnderTest, getPartitionsForTopic(_, UsedTopicName)).TIMES(1);
  REQUIRE_CALL(*UnderTest, getPartitionsForTopicInternal(_, UsedTopicName, _))
      .TIMES(1)
      .THROW(MetadataException("Test"));
  UnderTest->getPartitionsForTopicBase(KafkaSettings, UsedTopicName);

  // Wait until we are done processing "getPartitionsForTopic
  std::promise<bool> Promise;
  auto Future = Promise.get_future();
  UnderTest->Executor.sendLowPriorityWork(
      [&Promise]() { Promise.set_value(true); });
  Future.wait();
}

TEST_F(TopicTest, GetPartitionsForTopicNoException) {
  auto UnderTest = createTestedInstance();
  std::vector<int> ReturnPartitions{2, 3};
  FORBID_CALL(*UnderTest, getPartitionsForTopic(_, _));
  REQUIRE_CALL(*UnderTest, getPartitionsForTopicInternal(_, UsedTopicName, _))
      .TIMES(1)
      .RETURN(ReturnPartitions);
  REQUIRE_CALL(*UnderTest,
               getOffsetsForPartitions(_, UsedTopicName, ReturnPartitions))
      .TIMES(1);
  UnderTest->getPartitionsForTopicBase(KafkaSettings, UsedTopicName);

  // Wait until we are done processing "getPartitionsForTopic
  std::promise<bool> Promise;
  auto Future = Promise.get_future();
  UnderTest->Executor.sendLowPriorityWork(
      [&Promise]() { Promise.set_value(true); });
  Future.wait();
}

TEST_F(TopicTest, GetOffsetsForPartitionsException) {
  auto UnderTest = createTestedInstance();
  std::vector<int> Partitions{2, 3};
  REQUIRE_CALL(*UnderTest,
               getOffsetsForPartitions(_, UsedTopicName, Partitions))
      .TIMES(1);
  REQUIRE_CALL(*UnderTest,
               getOffsetForTimeInternal(_, UsedTopicName, Partitions, _, _))
      .TIMES(1)
      .THROW(MetadataException("Test"));
  UnderTest->getOffsetsForPartitionsBase(KafkaSettings, UsedTopicName,
                                         Partitions);

  // Wait until we are done processing "getOffsetsForPartitions
  std::promise<bool> Promise;
  auto Future = Promise.get_future();
  UnderTest->Executor.sendLowPriorityWork(
      [&Promise]() { Promise.set_value(true); });
  Future.wait();
}

TEST_F(TopicTest, GetOffsetsForPartitionsNoException) {
  auto UnderTest = createTestedInstance();
  std::vector<int> Partitions{6, 3, 2};
  TopicStandIn::offset_list ReturnOffsets{{1, 5}, {3, 6}};
  FORBID_CALL(*UnderTest, getOffsetsForPartitions(_, _, _));
  REQUIRE_CALL(*UnderTest,
               getOffsetForTimeInternal(_, UsedTopicName, Partitions, _, _))
      .TIMES(1)
      .RETURN(ReturnOffsets);
  REQUIRE_CALL(*UnderTest, createStreams(_, UsedTopicName, ReturnOffsets))
      .TIMES(1);
  UnderTest->getOffsetsForPartitionsBase(KafkaSettings, UsedTopicName,
                                         Partitions);

  // Wait until we are done processing "getOffsetsForPartitions
  std::promise<bool> Promise;
  auto Future = Promise.get_future();
  UnderTest->Executor.sendLowPriorityWork(
      [&Promise]() { Promise.set_value(true); });
  Future.wait();
}

TEST_F(TopicTest, CreateStreams) {
  auto UnderTest = createTestedInstance();
  TopicStandIn::offset_list PartitionOffsets{{1, 5}, {3, 6}};
  UnderTest->createStreamsBase(KafkaSettings, UsedTopicName, PartitionOffsets);
  ASSERT_EQ(PartitionOffsets.size(), UnderTest->ConsumerThreads.size());
  for (size_t i = 0; i < PartitionOffsets.size(); i++) {
    EXPECT_EQ(UnderTest->ConsumerThreads[i]->getPartitionID(),
              PartitionOffsets[i].first);
    EXPECT_EQ(UnderTest->ConsumerThreads[i]->getTopicName(), UsedTopicName);
  }
}
