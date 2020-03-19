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
  PartitionStandIn(std::unique_ptr<KafkaW::ConsumerInterface> Consumer,
                   int Partition, std::string TopicName,
                   Stream::SrcToDst const &Map, Stream::MessageWriter *Writer,
                   Metrics::Registrar RegisterMetric, Stream::time_point Start,
                   Stream::time_point Stop, Stream::duration StopLeeway,
                   Stream::duration KafkaErrorTimeout)
      : Stream::Partition(std::move(Consumer), Partition, std::move(TopicName),
                          std::move(Map), Writer, RegisterMetric, Start, Stop,
                          StopLeeway, KafkaErrorTimeout) {}
  MAKE_MOCK0(pollForMessage, void(), override);
  MAKE_MOCK0(addPollTask, void(), override);
  MAKE_MOCK1(processMessage, void(FileWriter::Msg const &), override);
  MAKE_MOCK1(shouldStopBasedOnPollStatus, bool(KafkaW::PollStatus), override);
  void pollForMessageBase() { Partition::pollForMessage(); }
  void addPollTaskBase() { Partition::addPollTask(); }
  using Partition::ConsumerPtr;
  using Partition::Executor;
  using Partition::MsgFilters;
  using Partition::processMessage;
  using Partition::StopTime;
  using Partition::StopTimeLeeway;
};

class ConsumerStandIn : public KafkaW::ConsumerInterface {
public:
  using PollReturnType = std::pair<KafkaW::PollStatus, FileWriter::Msg>;
  MAKE_MOCK1(addTopic, void(std::string const &), override);
  MAKE_MOCK2(addTopicAtTimestamp,
             void(std::string const &, std::chrono::milliseconds), override);
  MAKE_MOCK0(poll, PollReturnType(), override);
  MAKE_MOCK1(topicPresent, bool(std::string const &), override);
  MAKE_MOCK1(queryTopicPartitions, std::vector<int32_t>(std::string const &),
             override);
  MAKE_MOCK2(offsetsForTimesAllPartitions,
             std::vector<int64_t>(std::string const &,
                                  std::chrono::milliseconds),
             override);
  MAKE_MOCK3(addPartitionAtOffset, void(std::string const &, int, int64_t),
             override);
  MAKE_MOCK2(getHighWatermarkOffset, int64_t(std::string const &, int32_t),
             override);
  MAKE_MOCK1(getCurrentOffsets, std::vector<int64_t>(std::string const &),
             override);
};

using std::chrono_literals::operator""s;

class PartitionTest : public ::testing::Test {
public:
  auto createTestedInstance() {
    auto Temp = std::make_unique<PartitionStandIn>(
        std::make_unique<ConsumerStandIn>(), UsedPartitionId, TopicName,
        UsedMap, nullptr, Registrar, Start, Stop, StopLeeway, ErrorTimeout);
    Consumer = dynamic_cast<ConsumerStandIn *>(Temp->ConsumerPtr.get());
    return Temp;
  }
  ConsumerStandIn *Consumer{nullptr};

  int UsedPartitionId{0};
  std::string TopicName{"some_topic"};
  Stream::SrcToDst UsedMap{Stream::SrcDstKey{1, nullptr, "some_name", "idid"}};
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
  EXPECT_EQ(UnderTest->StopTime, Stop - StopLeeway);
}

TEST_F(PartitionTest, StartPolling) {
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*UnderTest, addPollTask()).TIMES(1);
  UnderTest->start();
}

TEST_F(PartitionTest, AddPollTask) {
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*UnderTest, pollForMessage()).TIMES(1);
  UnderTest->addPollTaskBase();

  // Wait until we are done processing
  std::promise<bool> Promise;
  auto Future = Promise.get_future();
  UnderTest->Executor.SendLowPrioWork(
      [&Promise]() { Promise.set_value(true); });
  Future.wait();
}

using trompeloeil::_;

TEST_F(PartitionTest, PollingEmptyMessage) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::Empty;
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  FORBID_CALL(*UnderTest, processMessage(_));
  REQUIRE_CALL(*UnderTest, addPollTask()).TIMES(1);
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(false);
  UnderTest->pollForMessageBase();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PollingWithMessage) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::Message;
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  REQUIRE_CALL(*UnderTest, processMessage(_)).TIMES(1);
  REQUIRE_CALL(*UnderTest, addPollTask()).TIMES(1);
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(false);
  UnderTest->pollForMessageBase();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PollingTimeoutMessage) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::TimedOut;
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  FORBID_CALL(*UnderTest, processMessage(_));
  REQUIRE_CALL(*UnderTest, addPollTask()).TIMES(1);
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(false);
  UnderTest->pollForMessageBase();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PollingErrorMessage) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::Error;
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  FORBID_CALL(*UnderTest, processMessage(_));
  REQUIRE_CALL(*UnderTest, addPollTask()).TIMES(1);
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(false);
  UnderTest->pollForMessageBase();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PollingEndOfPartitionMessage) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::EndOfPartition;
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  FORBID_CALL(*UnderTest, processMessage(_));
  REQUIRE_CALL(*UnderTest, addPollTask()).TIMES(1);
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(false);
  UnderTest->pollForMessageBase();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PollingWithMessageNoFilter) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::Message;
  auto UnderTest = createTestedInstance();
  UnderTest->MsgFilters.clear();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  REQUIRE_CALL(*UnderTest, processMessage(_)).TIMES(1);
  FORBID_CALL(*UnderTest, addPollTask());
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(false);
  UnderTest->pollForMessageBase();
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PollingWithMessageBeforeStart) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::Message;
  PollReturn.second.MetaData.Timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (Start - 10s).time_since_epoch());
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  REQUIRE_CALL(*UnderTest, processMessage(_)).TIMES(1);
  REQUIRE_CALL(*UnderTest, addPollTask()).TIMES(1);
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(false);
  UnderTest->pollForMessageBase();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PollingWithMessageAfterStart) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::Message;
  PollReturn.second.MetaData.Timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (Start + 10s).time_since_epoch());
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  REQUIRE_CALL(*UnderTest, processMessage(_)).TIMES(1);
  REQUIRE_CALL(*UnderTest, addPollTask()).TIMES(1);
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(false);
  UnderTest->pollForMessageBase();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PollingWithMessageAfterStop) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::Message;
  Stop = Start + 20s;
  PollReturn.second.MetaData.Timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (Stop + 5s).time_since_epoch());
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  REQUIRE_CALL(*UnderTest, processMessage(_)).TIMES(1);
  REQUIRE_CALL(*UnderTest, addPollTask()).TIMES(1);
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(false);
  UnderTest->pollForMessageBase();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PollingWithMessageAfterStopPlusLeeway) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::Message;
  Stop = Start + 20s;
  PollReturn.second.MetaData.Timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (Stop + 5s + StopLeeway).time_since_epoch());
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  REQUIRE_CALL(*UnderTest, processMessage(_)).TIMES(1);
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(false);
  FORBID_CALL(*UnderTest, addPollTask());
  UnderTest->pollForMessageBase();
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PollingWithErrorState) {
  ConsumerStandIn::PollReturnType PollReturn;
  PollReturn.first = KafkaW::PollStatus::Error;
  Stop = Start + 20s;
  PollReturn.second.MetaData.Timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (Stop + 5s + StopLeeway).time_since_epoch());
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  REQUIRE_CALL(*UnderTest, shouldStopBasedOnPollStatus(_))
      .TIMES(1)
      .RETURN(true);
  FORBID_CALL(*UnderTest, addPollTask());
  UnderTest->pollForMessageBase();
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, SetStopTime) {
  auto NewStopTime = Start + 12445s;
  auto UnderTest = createTestedInstance();
  for (auto &CFilter : UnderTest->MsgFilters) {
    EXPECT_EQ(CFilter.second->getStopTime(), Stop);
  }
  UnderTest->setStopTime(NewStopTime);
  // Wait until we are done processing
  std::promise<bool> Promise;
  auto Future = Promise.get_future();
  UnderTest->Executor.SendWork([&Promise]() { Promise.set_value(true); });
  Future.wait();
  for (auto &CFilter : UnderTest->MsgFilters) {
    EXPECT_EQ(CFilter.second->getStopTime(), NewStopTime);
  }
}
