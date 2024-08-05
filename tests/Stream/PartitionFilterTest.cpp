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
#include "Stream/Clock.h"
#include "Stream/PartitionFilter.h"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

class PartitionFilterTest : public ::testing::Test {
public:
  Stream::PartitionFilter UnderTest;
  FakeClock *clock_ptr{nullptr};

  void SetUp() override {
    auto fake_clock = std::make_unique<FakeClock>();
    fake_clock->set_time(time_point(std::chrono::seconds(0)));
    clock_ptr = fake_clock.get();
    UnderTest = {time_point::max(), 10ms, 20ms, std::move(fake_clock)};
  }
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
  clock_ptr->set_time(clock_ptr->get_current_time() + 40ms);
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
  clock_ptr->set_time(clock_ptr->get_current_time() + 40ms);
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
  clock_ptr->set_time(clock_ptr->get_current_time() + 40ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  EXPECT_TRUE(UnderTest.hasErrorState());
}

TEST_F(PartitionFilterTest, AfterErrorStateRecoversOnStopWithinLeeway) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
  clock_ptr->set_time(clock_ptr->get_current_time() + 40ms);
  EXPECT_TRUE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnMessageWithinLeeway) {
  UnderTest.setStopTime(clock_ptr->get_current_time() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Message));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnErrorWithinLeeway) {
  UnderTest.setStopTime(clock_ptr->get_current_time() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnEOPWithinLeeway) {
  UnderTest.setStopTime(clock_ptr->get_current_time() - 5ms);
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(Kafka::PollStatus::EndOfPartition));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnTimeOutWithinLeeway) {
  UnderTest.setStopTime(clock_ptr->get_current_time() - 5ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::TimedOut));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnMessageOutsideLeeway) {
  UnderTest.setStopTime(clock_ptr->get_current_time() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Message));
}

TEST_F(PartitionFilterTest, PartitionShouldNotStopOnEOPOutsideLeeway) {
  UnderTest.setStopTime(clock_ptr->get_current_time() - 15ms);
  EXPECT_FALSE(
      UnderTest.shouldStopPartition(Kafka::PollStatus::EndOfPartition));
}

TEST_F(PartitionFilterTest,
       PartitionShouldStopOnTimeOutOutsideLeewayAndAfterEOP) {
  UnderTest.setStopTime(clock_ptr->get_current_time() - 15ms);
  [[maybe_unused]] auto ignore =
      UnderTest.shouldStopPartition(Kafka::PollStatus::EndOfPartition);
  EXPECT_TRUE(UnderTest.shouldStopPartition(Kafka::PollStatus::TimedOut));
}

TEST_F(PartitionFilterTest,
       PartitionShouldNotStopOnTimeOutOutsideLeewayIfNoEOPFirst) {
  UnderTest.setStopTime(clock_ptr->get_current_time() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::TimedOut));
}

TEST_F(PartitionFilterTest, PartitionShouldStopNotOnErrorOutsideLeeway) {
  UnderTest.setStopTime(clock_ptr->get_current_time() - 15ms);
  EXPECT_FALSE(UnderTest.shouldStopPartition(Kafka::PollStatus::Error));
}
