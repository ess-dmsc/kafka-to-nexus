// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Status.h"

#include <gtest/gtest.h>
#include <hdf5.h>
#include <random>

using MessageInfo = FileWriter::Status::MessageInfo;
using StreamMasterInfo = FileWriter::Status::StreamMasterInfo;

constexpr uint64_t NumMessages{10000};
constexpr uint64_t NumErrors{10000};

double RandomGaussian() {
  static std::default_random_engine Generator;
  static std::normal_distribution<double> Normal(0.0, 1.0);
  return Normal(Generator);
}

TEST(MessageInfo, everythingIsZeroAtInitialisation) {
  MessageInfo MsgInfo;
  ASSERT_EQ(MsgInfo.getNumberMessages(), 0u);
  ASSERT_DOUBLE_EQ(MsgInfo.getMbytes(), 0.0);
  ASSERT_EQ(MsgInfo.getNumberWriteErrors(), 0u);
  ASSERT_EQ(MsgInfo.getNumberValidationErrors(), 0u);
  ASSERT_EQ(MsgInfo.getNumberProcessedMessages(), 0u);
}

TEST(MessageInfo, incrementingProcessedMessageGivesCorrectStats) {
  MessageInfo MsgInfo;
  const double NewMessageBytes{1024};
  MsgInfo.incrementProcessedCount(NewMessageBytes);

  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes(), NewMessageBytes * 1e-6);
  EXPECT_EQ(MsgInfo.getNumberProcessedMessages(), 1u);
}

TEST(MessageInfo, incrementingTotalMessageCountWorks) {
  MessageInfo MsgInfo;

  MsgInfo.incrementTotalMessageCount();
  MsgInfo.incrementTotalMessageCount();

  EXPECT_EQ(2u, MsgInfo.getNumberMessages());
}

TEST(MessageInfo, addingWriteErrorsGivesCorrectStats) {
  MessageInfo MsgInfo;
  MsgInfo.incrementWriteError();
  MsgInfo.incrementWriteError();

  EXPECT_EQ(MsgInfo.getNumberWriteErrors(), 2u);
}

TEST(MessageInfo, processingMultipleMessagesGetsCorrectStats) {
  MessageInfo MsgInfo;

  std::vector<double> MessageSizes = {600, 470, 170, 430, 300};

  for (auto Msg : MessageSizes) {
    MsgInfo.incrementProcessedCount(Msg);
  }

  EXPECT_EQ(MsgInfo.getNumberProcessedMessages(), MessageSizes.size());
}

TEST(StreamMasterInfo, addInfoFromOneStreamer) {
  StreamMasterInfo Info;
  MessageInfo MsgInfo;
  const double MessageBytes{1000};

  for (uint64_t i = 0; i < NumMessages; ++i) {
    MsgInfo.incrementProcessedCount(MessageBytes);
  }

  for (uint64_t i = 0; i < NumErrors; ++i) {
    MsgInfo.incrementWriteError();
  }
  Info.add(MsgInfo);

  EXPECT_NEAR(Info.getMbytes(), NumMessages * MessageBytes * 1e-6, 1e-6);
  EXPECT_EQ(Info.getNumberProcessedMessages(), NumMessages);
  EXPECT_EQ(Info.getNumberErrors(), NumErrors);
  EXPECT_EQ(Info.getTimeToNextMessage(), std::chrono::milliseconds{0});
}

TEST(StreamMasterInfo, accumulateInfoFromManyStreamers) {
  const int NumStreamers{13};
  StreamMasterInfo Info;

  double TotalMessages{0.0};
  double TotalSize{0.0};
  double TotalErrors{0.0};

  for (int s = 0; s < NumStreamers; ++s) {
    MessageInfo MsgInfo;
    for (uint64_t MessageCounter = 0; MessageCounter < NumMessages;
         ++MessageCounter) {
      auto MessageSize = std::fabs(RandomGaussian());
      MsgInfo.incrementProcessedCount(MessageSize);

      TotalMessages += 1.0;
      TotalSize += MessageSize * 1e-6;
    }
    for (uint64_t ErrorCounter = 0; ErrorCounter < NumErrors; ++ErrorCounter) {
      MsgInfo.incrementWriteError();
      TotalErrors += 1.0;
    }
    Info.add(MsgInfo);
  }
  EXPECT_DOUBLE_EQ(Info.getNumberProcessedMessages(), TotalMessages);
  EXPECT_NEAR(Info.getMbytes(), TotalSize, 1e-6);
  EXPECT_DOUBLE_EQ(Info.getNumberErrors(), TotalErrors);
}

TEST(MessageInfo, resettingTheStatisticsZeroesValues) {
  MessageInfo MsgInfo;

  std::vector<double> MessageSizes = {600, 470, 170, 430, 300};

  for (auto Msg : MessageSizes) {
    MsgInfo.incrementProcessedCount(Msg);
  }

  MsgInfo.resetStatistics();

  EXPECT_EQ(0u, MsgInfo.getMbytes());
  EXPECT_EQ(0u, MsgInfo.getNumberMessages());
  EXPECT_EQ(0u, MsgInfo.getNumberProcessedMessages());
  EXPECT_EQ(0u, MsgInfo.getNumberWriteErrors());
}
