// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Kafka/BrokerSettings.h"
#include "Kafka/Consumer.h"
#include "Kafka/MetadataException.h"
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
  TopicStandIn(Kafka::BrokerSettings Settings, std::string const &Topic,
               Stream::SrcToDst Map, Stream::MessageWriter *Writer,
               Metrics::Registrar &RegisterMetric, time_point StartTime,
               duration StartTimeLeeway, time_point StopTime,
               duration StopTimeLeeway,
               std::unique_ptr<Kafka::ConsumerFactoryInterface> CreateConsumers)
      : Stream::Topic(Settings, Topic, Map, Writer, RegisterMetric, StartTime,
                      StartTimeLeeway, StopTime, StopTimeLeeway,
                      std::move(CreateConsumers)) {}
  virtual void checkIfDoneTask() override {
    // Do nothing to prevent repeated calling of checkIfDone()
  }
  using Topic::checkIfDone;
  using Topic::ConsumerThreads;
  using Topic::CurrentMetadataTimeOut;
  using Topic::Executor;
  using offset_list = std::vector<std::pair<int, int64_t>>;
  MAKE_CONST_MOCK6(getOffsetForTimeInternal,
                   offset_list(std::string const &, std::string const &,
                               std::vector<int> const &, time_point, duration,
                               Kafka::BrokerSettings BrokerSettings),
                   override);
  MAKE_CONST_MOCK4(getPartitionsForTopicInternal,
                   std::vector<int>(std::string const &, std::string const &,
                                    duration,
                                    Kafka::BrokerSettings BrokerSettings),
                   override);
  MAKE_MOCK2(getPartitionsForTopic,
             void(Kafka::BrokerSettings const &, std::string const &),
             override);
  MAKE_MOCK3(getOffsetsForPartitions,
             void(Kafka::BrokerSettings const &, std::string const &,
                  std::vector<int> const &),
             override);
  MAKE_MOCK3(createStreams,
             void(Kafka::BrokerSettings const &, std::string const &,
                  std::vector<std::pair<int, int64_t>> const &),
             override);
  MAKE_MOCK0(shouldGiveUp, bool(), override);
  MAKE_CONST_MOCK0(getCurrentTime, time_point(), override);
  void initMetadataCalls(Kafka::BrokerSettings const &,
                         std::string const &) override {}
  void initMetadataCallsBase(Kafka::BrokerSettings const &Settings,
                             std::string const &Topic) {
    Topic::initMetadataCalls(Settings, Topic);
  }

  void getPartitionsForTopicBase(Kafka::BrokerSettings const &Settings,
                                 std::string const &Topic) {
    Topic::getPartitionsForTopic(Settings, Topic);
  }

  void getOffsetsForPartitionsBase(Kafka::BrokerSettings const &Settings,
                                   std::string const &Topic,
                                   std::vector<int> const &Partitions) {
    Topic::getOffsetsForPartitions(Settings, Topic, Partitions);
  }

  auto shouldGiveUpBase() { return Topic::shouldGiveUp(); }

  void createStreamsBase(
      Kafka::BrokerSettings const &Settings, std::string const &Topic,
      std::vector<std::pair<int, int64_t>> const &PartitionOffsets) {
    Topic::createStreams(Settings, Topic, PartitionOffsets);
  }
};

void waitUntilDoneProcessing(TopicStandIn *UnderTest) {
  // Queue a job in the executor and block until it is complete
  // so that we know previously queued job that is part of test should
  // now have been executed
  std::promise<bool> Promise;
  auto Future = Promise.get_future();
  UnderTest->Executor.sendLowPriorityWork(
      [&Promise]() { Promise.set_value(true); });
  Future.wait();
}

class TopicTest : public ::testing::Test {
public:
  auto createTestedInstance() {
    return std::make_unique<TopicStandIn>(
        KafkaSettings, UsedTopicName, Map, nullptr, Registrar, Start, 5s, Stop,
        5s, std::make_unique<Kafka::StubConsumerFactory>());
  }
  std::string const UsedTopicName{"some_topic_or_another"};
  Kafka::BrokerSettings KafkaSettings;
  time_point Start{std::chrono::system_clock::now()};
  time_point Stop{std::chrono::system_clock::time_point::max()};
  Metrics::Registrar Registrar{"some_name", {}};
  Stream::SrcToDst Map;
};

using std::chrono_literals::operator""ms;
using trompeloeil::_;

TEST_F(TopicTest, StartMetaDataCall) {
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*UnderTest, getPartitionsForTopic(_, UsedTopicName)).TIMES(1);
  UnderTest->initMetadataCallsBase(KafkaSettings, UsedTopicName);

  waitUntilDoneProcessing(UnderTest.get());
}

TEST_F(TopicTest, IfGetPartitionsForTopicExceptionThenReExecute) {
  auto UnderTest = createTestedInstance();

  // The metadata request can time out, so it is important to retry if
  // unsuccessful
  REQUIRE_CALL(*UnderTest, shouldGiveUp()).TIMES(1).RETURN(false);
  REQUIRE_CALL(*UnderTest, getPartitionsForTopic(_, UsedTopicName)).TIMES(1);
  REQUIRE_CALL(*UnderTest,
               getPartitionsForTopicInternal(_, UsedTopicName, _, _))
      .TIMES(1)
      .THROW(MetadataException("Test"));
  UnderTest->getPartitionsForTopicBase(KafkaSettings, UsedTopicName);

  waitUntilDoneProcessing(UnderTest.get());
}

TEST_F(TopicTest, GetPartitionsForTopicTimeOut) {
  auto UnderTest = createTestedInstance();

  // The metadata request can time out, so it is important to retry if
  // unsuccessful
  FORBID_CALL(*UnderTest, getPartitionsForTopic(_, _));
  REQUIRE_CALL(*UnderTest, shouldGiveUp()).TIMES(1).RETURN(true);
  REQUIRE_CALL(*UnderTest,
               getPartitionsForTopicInternal(_, UsedTopicName, _, _))
      .TIMES(1)
      .THROW(MetadataException("Test"));
  UnderTest->getPartitionsForTopicBase(KafkaSettings, UsedTopicName);

  waitUntilDoneProcessing(UnderTest.get());
}

TEST_F(TopicTest, IfGetPartitionsForTopicSuccessThenNotReExecuted) {
  auto UnderTest = createTestedInstance();
  std::vector<int> ReturnPartitions{2, 3};

  FORBID_CALL(*UnderTest, getPartitionsForTopic(_, _));
  REQUIRE_CALL(*UnderTest,
               getPartitionsForTopicInternal(_, UsedTopicName, _, _))
      .TIMES(1)
      .RETURN(ReturnPartitions);
  REQUIRE_CALL(*UnderTest,
               getOffsetsForPartitions(_, UsedTopicName, ReturnPartitions))
      .TIMES(1);
  UnderTest->getPartitionsForTopicBase(KafkaSettings, UsedTopicName);

  waitUntilDoneProcessing(UnderTest.get());
}

TEST_F(TopicTest, IfGetOffsetsForPartitionsExceptionThenReExecute) {
  auto UnderTest = createTestedInstance();
  std::vector<int> Partitions{2, 3};

  // The query to the broker can time out, so it is important to retry if
  // unsuccessful
  REQUIRE_CALL(*UnderTest, shouldGiveUp()).TIMES(1).RETURN(false);
  REQUIRE_CALL(*UnderTest,
               getOffsetsForPartitions(_, UsedTopicName, Partitions))
      .TIMES(1);
  REQUIRE_CALL(*UnderTest,
               getOffsetForTimeInternal(_, UsedTopicName, Partitions, _, _, _))
      .TIMES(1)
      .THROW(MetadataException("Test"));
  UnderTest->getOffsetsForPartitionsBase(KafkaSettings, UsedTopicName,
                                         Partitions);

  waitUntilDoneProcessing(UnderTest.get());
}

TEST_F(TopicTest, GetOffsetsForPartitionsTimeOut) {
  auto UnderTest = createTestedInstance();
  std::vector<int> Partitions{2, 3};

  // The query to the broker can time out, so it is important to retry if
  // unsuccessful
  FORBID_CALL(*UnderTest, getOffsetsForPartitions(_, _, _));
  REQUIRE_CALL(*UnderTest, shouldGiveUp()).TIMES(1).RETURN(true);
  REQUIRE_CALL(*UnderTest,
               getOffsetForTimeInternal(_, UsedTopicName, Partitions, _, _, _))
      .TIMES(1)
      .THROW(MetadataException("Test"));
  UnderTest->getOffsetsForPartitionsBase(KafkaSettings, UsedTopicName,
                                         Partitions);

  waitUntilDoneProcessing(UnderTest.get());
}

TEST_F(TopicTest, IfGetOffsetsForPartitionsSuccessThenNotReExecuted) {
  auto UnderTest = createTestedInstance();
  std::vector<int> Partitions{6, 3, 2};
  TopicStandIn::offset_list ReturnOffsets{{1, 5}, {3, 6}};

  FORBID_CALL(*UnderTest, getOffsetsForPartitions(_, _, _));
  REQUIRE_CALL(*UnderTest,
               getOffsetForTimeInternal(_, UsedTopicName, Partitions, _, _, _))
      .TIMES(1)
      .RETURN(ReturnOffsets);
  REQUIRE_CALL(*UnderTest, createStreams(_, UsedTopicName, ReturnOffsets))
      .TIMES(1);
  UnderTest->getOffsetsForPartitionsBase(KafkaSettings, UsedTopicName,
                                         Partitions);

  waitUntilDoneProcessing(UnderTest.get());
}

TEST_F(TopicTest, ShouldNotGiveUp) {
  auto UnderTest = createTestedInstance();

  REQUIRE_CALL(*UnderTest, getCurrentTime())
      .TIMES(1)
      .RETURN(system_clock::now());
  EXPECT_FALSE(UnderTest->shouldGiveUpBase());
}

using std::chrono_literals::operator""h;

TEST_F(TopicTest, ShouldGiveUp) {
  auto UnderTest = createTestedInstance();

  REQUIRE_CALL(*UnderTest, getCurrentTime())
      .TIMES(1)
      .RETURN(system_clock::now() + duration(1h));
  EXPECT_TRUE(UnderTest->shouldGiveUpBase());
}

TEST_F(TopicTest, StreamsAreCreatedCorrespondingToQueriedPartitions) {
  auto UnderTest = createTestedInstance();
  UnderTest->start();
  TopicStandIn::offset_list PartitionOffsets{{1, 5}, {3, 6}};
  UnderTest->createStreamsBase(KafkaSettings, UsedTopicName, PartitionOffsets);
  ASSERT_EQ(PartitionOffsets.size(), UnderTest->ConsumerThreads.size());
  for (auto &Partition : UnderTest->ConsumerThreads) {
    auto CPartitionId = Partition->getPartitionID();
    EXPECT_TRUE(std::any_of(PartitionOffsets.begin(), PartitionOffsets.end(),
                            [&CPartitionId](auto const &Item) {
                              return Item.first == CPartitionId;
                            }));
    EXPECT_EQ(Partition->getTopicName(), UsedTopicName);
  }
}

class PartitionStandInAlt : public Stream::Partition {
public:
  PartitionStandInAlt()
      : Stream::Partition({}, 0, "", {}, nullptr, Metrics::Registrar("", {}),
                          {}, {}, {}, {}) {}
  MAKE_CONST_MOCK0(hasFinished, bool(), override);
};

TEST_F(TopicTest, TopicIsNotDoneIfAnyOfItsPartitionsAreNotFinished) {
  auto UnderTest = createTestedInstance();

  auto Partition1 = std::make_unique<PartitionStandInAlt>();
  auto Partition1Ptr = Partition1.get();
  UnderTest->ConsumerThreads.emplace_back(std::move(Partition1));
  REQUIRE_CALL(*Partition1Ptr, hasFinished()).TIMES(1).RETURN(true);

  auto Partition2 = std::make_unique<PartitionStandInAlt>();
  auto Partition2Ptr = Partition2.get();
  UnderTest->ConsumerThreads.emplace_back(std::move(Partition2));
  REQUIRE_CALL(*Partition2Ptr, hasFinished()).TIMES(1).RETURN(false);

  UnderTest->checkIfDone();
  EXPECT_FALSE(UnderTest->isDone());
}

TEST_F(TopicTest, TopicIsDoneIfAllItsPartitionsAreFinished) {
  auto UnderTest = createTestedInstance();

  auto Partition1 = std::make_unique<PartitionStandInAlt>();
  auto Partition1Ptr = Partition1.get();
  UnderTest->ConsumerThreads.emplace_back(std::move(Partition1));
  REQUIRE_CALL(*Partition1Ptr, hasFinished()).TIMES(1).RETURN(true);

  auto Partition2 = std::make_unique<PartitionStandInAlt>();
  auto Partition2Ptr = Partition2.get();
  UnderTest->ConsumerThreads.emplace_back(std::move(Partition2));
  REQUIRE_CALL(*Partition2Ptr, hasFinished()).TIMES(1).RETURN(true);

  UnderTest->checkIfDone();
  EXPECT_TRUE(UnderTest->isDone());
}
