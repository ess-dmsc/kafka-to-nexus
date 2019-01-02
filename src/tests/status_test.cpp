#include "Status.h"

#include <gtest/gtest.h>

#include <random>

using MessageInfo = FileWriter::Status::MessageInfo;
using StreamMasterInfo = FileWriter::Status::StreamMasterInfo;

constexpr int NumMessages{10000};
constexpr int NumErrors{10000};

double RandomGaussian() {
  static std::default_random_engine Generator;
  static std::normal_distribution<double> Normal(0.0, 1.0);
  return Normal(Generator);
}

TEST(MessageInfo, everythingIsZeroAtInitialisation) {
  MessageInfo MsgInfo;
  ASSERT_DOUBLE_EQ(MsgInfo.getMessages().first, 0.0);
  ASSERT_DOUBLE_EQ(MsgInfo.getMessages().second, 0.0);
  ASSERT_DOUBLE_EQ(MsgInfo.getMbytes().first, 0.0);
  ASSERT_DOUBLE_EQ(MsgInfo.getMbytes().second, 0.0);
  ASSERT_DOUBLE_EQ(MsgInfo.getErrors(), 0.0);
}

TEST(MessageInfo, addOneMessage) {
  MessageInfo MsgInfo;
  const double NewMessageBytes{1024};
  MsgInfo.newMessage(NewMessageBytes);

  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().first, 1.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().second, 1.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes().first, NewMessageBytes * 1e-6);
  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes().second,
                   NewMessageBytes * NewMessageBytes * 1e-12);
  EXPECT_DOUBLE_EQ(MsgInfo.getErrors(), 0.0);
}

TEST(MessageInfo, addOneError) {
  MessageInfo MsgInfo;
  MsgInfo.error();

  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().first, 0.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().second, 0.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes().first, 0.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes().second, 0.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getErrors(), 1.0);
}

TEST(MessageInfo, addMessages) {
  MessageInfo MsgInfo;

  double accum{0.0}, accum2{0.0};
  for (int i = 0; i < NumMessages; ++i) {
    auto new_message_bytes = std::fabs(RandomGaussian());
    accum += new_message_bytes;
    accum2 += new_message_bytes * new_message_bytes;
    MsgInfo.newMessage(new_message_bytes);
  }

  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().first, NumMessages);
  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().second, NumMessages);
  EXPECT_NEAR(MsgInfo.getMbytes().first, accum * 1e-6, 1e-6);
  EXPECT_NEAR(MsgInfo.getMbytes().second, accum2 * 1e-12, 1e-12);
  EXPECT_DOUBLE_EQ(MsgInfo.getErrors(), 0.0);
}

TEST(StreamMasterInfo, everythingIsZeroAtInitialisation) {
  StreamMasterInfo Info;
  EXPECT_EQ(Info.getMbytes().first, 0.0);
  EXPECT_EQ(Info.getMbytes().second, 0.0);
  EXPECT_EQ(Info.getMessages().first, 0.0);
  EXPECT_EQ(Info.getMessages().second, 0.0);
  EXPECT_EQ(Info.getErrors(), 0.0);
  EXPECT_EQ(Info.getTimeToNextMessage(), std::chrono::milliseconds{0});
}

TEST(StreamMasterInfo, addOneInfo) {
  StreamMasterInfo Info;
  MessageInfo MsgInfo;
  const double MessageBytes{1000};

  for (int i = 0; i < NumMessages; ++i) {
    MsgInfo.newMessage(MessageBytes);
  }
  for (int i = 0; i < NumErrors; ++i) {
    MsgInfo.error();
  }
  Info.add(MsgInfo);

  EXPECT_NEAR(Info.getMbytes().first, NumMessages * MessageBytes * 1e-6, 1e-6);
  EXPECT_NEAR(Info.getMbytes().second,
              NumMessages * (MessageBytes * 1e-6) * (MessageBytes * 1e-6),
              1e-6);
  EXPECT_EQ(Info.getMessages().first, NumMessages);
  EXPECT_EQ(Info.getMessages().second, NumMessages);
  EXPECT_EQ(Info.getErrors(), NumErrors);
  EXPECT_EQ(Info.getTimeToNextMessage(), std::chrono::milliseconds{0});
}

TEST(StreamMasterInfo, accumulateInfos) {
  const size_t NumStreamers{13};
  StreamMasterInfo Info;

  double TotalMessages{0.0}, TotalMessages2{0.0};
  double TotalSize{0.0}, TotalSize2{0.0};
  double TotalErrors{0.0};

  for (size_t i = 0; i < NumStreamers; ++i) {
    MessageInfo MsgInfo;
    for (int j = 0; j < NumMessages; ++j) {
      auto MessageSize = std::fabs(RandomGaussian());
      MsgInfo.newMessage(MessageSize);

      TotalMessages += 1.0;
      TotalMessages2 += 1.0;
      TotalSize += MessageSize * 1e-6;
      TotalSize2 += MessageSize * MessageSize * 1e-12;
    }
    for (int k = 0; k < NumErrors; ++k) {
      MsgInfo.error();
      TotalErrors += 1.0;
    }
    Info.add(MsgInfo);
  }
  EXPECT_DOUBLE_EQ(Info.getMessages().first, TotalMessages);
  EXPECT_DOUBLE_EQ(Info.getMessages().second, TotalMessages2);
  EXPECT_NEAR(Info.getMbytes().first, TotalSize, 1e-6);
  EXPECT_NEAR(Info.getMbytes().second, TotalSize2, 1e-12);
  EXPECT_DOUBLE_EQ(Info.getErrors(), TotalErrors);
}

TEST(MessageInfo, computeDerivedQuantities) {
  const std::vector<double> MessagesSize{1.0, 2.0, 3.0, 4.0, 5.0};
  MessageInfo MsgInfo;

  for (auto &m : MessagesSize) {
    MsgInfo.newMessage(m * 1e6);
  }
  std::chrono::milliseconds Duration(1000);

  auto Size = FileWriter::Status::messageSize(MsgInfo);
  auto Frequency = FileWriter::Status::messageFrequency(MsgInfo, Duration);
  auto Throughput = FileWriter::Status::messageThroughput(MsgInfo, Duration);
  EXPECT_DOUBLE_EQ(Size.first, 3.0);
  EXPECT_NEAR(Size.second, 1.5811388300841898, 10e-3); // unbiased
  EXPECT_NEAR(Frequency, 1e3 * MessagesSize.size() / Duration.count(), 10e-3);
  EXPECT_NEAR(Frequency, 1e3 * MessagesSize.size() / Duration.count(), 10e-3);
  EXPECT_NEAR(Throughput, 1e3 * std::accumulate(MessagesSize.begin(),
                                                MessagesSize.end(), 0.0) /
                              Duration.count(),
              10e-3);
}

TEST(MessageInfo, derivedQuantitiesAreZeroIfFactorIsNull) {
  const std::vector<double> MessagesSize{1.0, 2.0, 3.0, 4.0, 5.0};
  MessageInfo MsgInfo;

  for (auto &m : MessagesSize) {
    MsgInfo.newMessage(m * 1e6);
  }
  std::chrono::milliseconds Duration(0);

  auto Frequency = FileWriter::Status::messageFrequency(MsgInfo, Duration);
  auto Throughput = FileWriter::Status::messageThroughput(MsgInfo, Duration);
  EXPECT_DOUBLE_EQ(0.0, Frequency);
  EXPECT_DOUBLE_EQ(0.0, Throughput);
}
