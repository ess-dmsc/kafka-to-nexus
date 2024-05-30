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

#include "Kafka/PollStatus.h"
#include "Stream/PartitionFilter.h"
#include "TimeUtility.h"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

class PartitionFilterTest : public ::testing::Test {
public:
  Stream::PartitionFilter UnderTest{time_point::max(), 10ms, 20ms};
};

TEST_F(PartitionFilterTest, NoErrorOnInitState) {
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, StopWhenForced) {
  UnderTest.forceStop();
  EXPECT_TRUE(UnderTest.shouldStopPartition(Kafka::PollStatus::Message));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, OnMessageNoStopFlagged) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Message));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, OnErrorStateNoStopFlagged) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  EXPECT_TRUE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateNoStopAlt) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  EXPECT_TRUE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, OnTimeoutHasErrorState) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::TimedOut));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorRecoversOnValidMessage) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Message));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorRecoversOnValidMessageWithDelay) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Message));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorRecoversOnEOP) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(Kafka::PollStatus::EndOfPartition));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorRecoversOnEOPWithDelay) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(Kafka::PollStatus::EndOfPartition));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorStateNoRecoveryOnTimeOut) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::TimedOut));
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  EXPECT_TRUE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorStateRecoverOnTimeOutWithDelay) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::TimedOut));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  EXPECT_TRUE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorStateRecoversOnStopWithinLeeway) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_TRUE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnMessageWithinLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Message));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnErrorWithinLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnEOPWithinLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(Kafka::PollStatus::EndOfPartition));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnTimeOutWithinLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::TimedOut));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnMessageOutsideLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Message));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnEOPOutsideLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(Kafka::PollStatus::EndOfPartition));
}

TEST_F(PartitionFilterTest,
       PartitionShouldStopOnTimeOutOutsideLeewayAndAfterEOP) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  [[maybe_unused]] auto ignore =
      UnderTest.shouldStopPartition(Kafka::PollStatus::EndOfPartition);
  EXPECT_TRUE(UnderTest.shouldStopPartition(Kafka::PollStatus::TimedOut));
}

TEST_F(PartitionFilterTest,
       PartitionShouldNotStopOnTimeOutOutsideLeewayIfNoEOPFirst) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::TimedOut));
}

TEST_F(PartitionFilterTest, PartitionShouldStopNotOnErrorOutsideLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
}
