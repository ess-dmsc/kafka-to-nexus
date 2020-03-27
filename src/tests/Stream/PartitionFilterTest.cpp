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

TEST_F(PartitionFilterTest, NoErrorOnInitState) {
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, OnMessageNoStopFlagged) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Message));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, OnErrorStateNoStopFlagged) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_TRUE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, ErrorStateNoStopAlt) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_TRUE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, OnTimeoutNoStopFlagged) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, OnEmptyNoStopFlagged) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorRecoversOnValidMessage) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Message));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorRecoversOnValidMessageWithDelay) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Message));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorRecoversOnEOP) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(KafkaW::PollStatus::EndOfPartition));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorRecoversOnEOPWithDelay) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(KafkaW::PollStatus::EndOfPartition));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorStateRecoversOnEmpty) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Empty));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorStateRecoversOnEmptyWithDelay) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Empty));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorStateRecoversOnTimeOut) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorStateRecoversOnTimeOutWithDelay) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
  EXPECT_FALSE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorStateRecoversOnStopWithinLeeway) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  std::this_thread::sleep_for(40ms);
  EXPECT_TRUE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnMessageWithinLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Message));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnErrorWithinLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnEmptyWithinLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Empty));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnEOPWithinLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(KafkaW::PollStatus::EndOfPartition));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnTimeOutWithinLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnMessageOutsideLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Message));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnEmptyOutsideLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Empty));
}

TEST_F(PartitionFilterTest, PartitionShouldStopOnEOPOutsideLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_TRUE(
      UnderTest.shouldStopPartition(KafkaW::PollStatus::EndOfPartition));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnTimeOutOutsideLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::TimedOut));
}

TEST_F(PartitionFilterTest, PartitionShouldStopNotOnErrorOutsideLeeway) {
  UnderTest.setStopTime(std::chrono::system_clock::now() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
}
