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

#include <gtest/gtest.h>
#include "Stream/PartitionFilter.h"
#include <chrono>

using std::chrono_literals::operator""ms;

class PartitionFilterTest : public ::testing::Test {
public:
  Stream::PartitionFilter UnderTest{Stream::time_point::max(), 50ms};
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

TEST_F(PartitionFilterTest, ErrorStateRecoveredEOP) {
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::Error));
  EXPECT_FALSE(UnderTest.shouldStopPartition(KafkaW::PollStatus::EndOfPartition));
  EXPECT_FALSE(UnderTest.hasErrorState());
}