#include "Status.h"

#include <gtest/gtest.h>
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
  ASSERT_DOUBLE_EQ(MsgInfo.getMessages(), 0u);
  ASSERT_DOUBLE_EQ(MsgInfo.getMbytes(), 0.0);
  ASSERT_DOUBLE_EQ(MsgInfo.getErrors(), 0u);
}

TEST(MessageInfo, addOneMessage) {
  MessageInfo MsgInfo;
  const double NewMessageBytes{1024};
  MsgInfo.newMessage(NewMessageBytes);
  auto Size = MsgInfo.messageSize();

  EXPECT_EQ(MsgInfo.getMessages(), 1u);
  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes(), NewMessageBytes * 1e-6);
  EXPECT_DOUBLE_EQ(Size.first, NewMessageBytes * 1e-6);
  EXPECT_DOUBLE_EQ(Size.second, 0);
  EXPECT_EQ(MsgInfo.getErrors(), 0u);
}

TEST(MessageInfo, addOneError) {
  MessageInfo MsgInfo;
  MsgInfo.error();

  EXPECT_EQ(MsgInfo.getMessages(), 0u);
  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes(), 0.0);
  EXPECT_EQ(MsgInfo.getErrors(), 1u);
}

TEST(MessageInfo, addManyMessages) {
  MessageInfo MsgInfo;

  double TotalMB{0.0}, TotalMBSquare{0.0};
  for (uint64_t i = 0; i < NumMessages; ++i) {
    auto MessageBytes = std::fabs(RandomGaussian());
    TotalMB += MessageBytes * 1e-6;
    TotalMBSquare += MessageBytes * MessageBytes * 1e-12;
    MsgInfo.newMessage(MessageBytes);
  }
  auto Size = MsgInfo.messageSize();

  EXPECT_EQ(MsgInfo.getMessages(), NumMessages);
  EXPECT_NEAR(Size.first, TotalMB / NumMessages, 1e-6);
  EXPECT_NEAR(Size.second, (TotalMBSquare - (TotalMB * TotalMB) / NumMessages) /
                               (NumMessages - 1),
              1e-6);
  EXPECT_EQ(MsgInfo.getErrors(), 0u);
}

TEST(StreamMasterInfo, addInfoFromOneStreamer) {
  StreamMasterInfo Info;
  MessageInfo MsgInfo;
  const double MessageBytes{1000};

  for (uint64_t i = 0; i < NumMessages; ++i) {
    MsgInfo.newMessage(MessageBytes);
  }
  for (uint64_t i = 0; i < NumErrors; ++i) {
    MsgInfo.error();
  }
  Info.add(MsgInfo);

  EXPECT_NEAR(Info.getMbytes(), NumMessages * MessageBytes * 1e-6, 1e-6);
  EXPECT_EQ(Info.getMessages(), NumMessages);
  EXPECT_EQ(Info.getErrors(), NumErrors);
  EXPECT_EQ(Info.getTimeToNextMessage(), std::chrono::milliseconds{0});
}

TEST(StreamMasterInfo, accumulateInfoFromManyStreamers) {
  const int NumStreamers{13};
  StreamMasterInfo Info;

  double TotalMessages{0.0}, TotalMessages2{0.0};
  double TotalSize{0.0}, TotalSize2{0.0};
  double TotalErrors{0.0};

  for (int s = 0; s < NumStreamers; ++s) {
    MessageInfo MsgInfo;
    for (uint64_t MessageCounter = 0; MessageCounter < NumMessages;
         ++MessageCounter) {
      auto MessageSize = std::fabs(RandomGaussian());
      MsgInfo.newMessage(MessageSize);

      TotalMessages += 1.0;
      TotalMessages2 += 1.0;
      TotalSize += MessageSize * 1e-6;
      TotalSize2 += MessageSize * MessageSize * 1e-12;
    }
    for (uint64_t ErrorCounter = 0; ErrorCounter < NumErrors; ++ErrorCounter) {
      MsgInfo.error();
      TotalErrors += 1.0;
    }
    Info.add(MsgInfo);
  }
  EXPECT_DOUBLE_EQ(Info.getMessages(), TotalMessages);
  EXPECT_NEAR(Info.getMbytes(), TotalSize, 1e-6);
  EXPECT_DOUBLE_EQ(Info.getErrors(), TotalErrors);
}

TEST(MessageInfo, computeDerivedQuantities) {
  const std::vector<double> MessagesSize{1.0, 2.0, 3.0, 4.0, 5.0};
  MessageInfo MsgInfo;

  for (auto &MessageSize : MessagesSize) {
    MsgInfo.newMessage(MessageSize * 1e6);
  }
  std::chrono::milliseconds Duration(1000);

  auto Size = MsgInfo.messageSize();
  EXPECT_DOUBLE_EQ(Size.first, 3.0);
  EXPECT_NEAR(Size.second, 1.5811388300841898, 10e-3);
}
