// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Stream/Partition.h"
#include "helpers/KafkaWMocks.h"
#include <gtest/gtest.h>

class PartitionStandIn : public Stream::Partition {
public:
  PartitionStandIn(std::unique_ptr<KafkaW::Consumer> Consumer, int Partition,
                   std::string TopicName, Stream::SrcToDst const &Map,
                   Stream::MessageWriter *Writer,
                   Metrics::Registrar RegisterMetric, Stream::time_point Start,
                   Stream::time_point Stop, Stream::duration StopLeeway,
                   Stream::duration KafkaErrorTimeout)
      : Stream::Partition(std::move(Consumer), Partition, std::move(TopicName),
                          std::move(Map), Writer, RegisterMetric, Start, Stop,
                          StopLeeway, KafkaErrorTimeout) {}
  MAKE_MOCK0(pollForMessage, void(), override);
  using Partition::Executor;
  using Partition::processMessage;
  using Partition::StopTime;
  using Partition::StopTimeLeeway;
};

using std::chrono_literals::operator""s;

class PartitionTest : public ::testing::Test {
public:
  auto createTestedInstance() {
    return std::make_unique<PartitionStandIn>(nullptr, UsedPartitionId,
                                              TopicName, UsedMap, nullptr,
                                              Registrar, Start, Stop, StopLeeway, ErrorTimeout);
  }
  MockKafkaConsumer Consumer;
  int UsedPartitionId{0};
  std::string TopicName{"some_topic"};
  Stream::SrcToDst UsedMap{};
  Stream::time_point Start{std::chrono::system_clock::now()};
  Stream::time_point Stop{std::chrono::system_clock::time_point::max()};
  Stream::duration StopLeeway{5s};
  Stream::duration ErrorTimeout{10s};
  Metrics::Registrar Registrar{"some_name", {}};
};

TEST_F(PartitionTest, InitValues) {
  auto UnderTest = createTestedInstance();
  EXPECT_EQ(UnderTest->getPartitionID(), UsedPartitionId);
  EXPECT_EQ(UnderTest->getTopicName(), TopicName);
  EXPECT_EQ(UnderTest->StopTimeLeeway, StopLeeway);
  EXPECT_EQ(UnderTest->StopTime, Stop);
}

TEST_F(PartitionTest, StartPolling) {
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*UnderTest, pollForMessage()).TIMES(1);
  UnderTest->start();

  // Wait until we are done processing
  std::promise<bool> Promise;
  auto Future = Promise.get_future();
  UnderTest->Executor.SendLowPrioWork(
      [&Promise]() { Promise.set_value(true); });
  Future.wait();
}
