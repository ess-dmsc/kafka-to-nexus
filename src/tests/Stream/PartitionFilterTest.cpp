// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \brief Test partition filtering.
///

#include "Stream/PartitionFilter.h"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

using std::chrono_literals::operator""ms;
using Stream::time_point;
class PartitionFilterTest : public ::testing::Test {
public:
  Stream::PartitionFilter UnderTest{Stream::time_point::max(), 10ms, 20ms};
};

TEST_F(PartitionFilterTest, InitState) {
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, MessageNoStop) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Message));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateNoStop) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_TRUE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateNoStopAlt) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_TRUE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, TimeoutNoStop) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, EmptyNoStop) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateRecovered) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Message));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateRecoveredAlt) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Message));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateRecoveredEOP) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(KafkaW::PollStatus::EndOfPartition));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateRecoveredEOPAlt) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(KafkaW::PollStatus::EndOfPartition));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateRecoveredEmpty) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Empty));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateRecoveredEmptyAlt) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Empty));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateRecoveredTimeOut) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateRecoveredTimeOutAlt) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateAndStop) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_TRUE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
}

TEST_F(PartitionFilterTest, StopTimeNoStopMessage) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Message));
}

TEST_F(PartitionFilterTest, StopTimeNoStopError) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
}

TEST_F(PartitionFilterTest, StopTimeNoStopEmpty) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Empty));
}

TEST_F(PartitionFilterTest, StopTimeNoStopEOP) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(KafkaW::PollStatus::EndOfPartition));
}

TEST_F(PartitionFilterTest, StopTimeNoStopTimeOut) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
}

TEST_F(PartitionFilterTest, StopTimeNoStopMessageAlt) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Message));
}

TEST_F(PartitionFilterTest, StopTimeNoStopEmptyAlt) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Empty));
}

TEST_F(PartitionFilterTest, StopTimeDoStopEOP) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_TRUE(
      UnderTest.shouldStopPartition(KafkaW::PollStatus::EndOfPartition));
}

TEST_F(PartitionFilterTest, StopTimeNoStopTimeOutAlt) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
}

TEST_F(PartitionFilterTest, StopTimeNoStopErrorAlt) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
}
