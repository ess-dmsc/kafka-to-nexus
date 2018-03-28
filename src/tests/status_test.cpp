#include "Status.h"

#include <gtest/gtest.h>

#include <random>

using MessageInfo = FileWriter::Status::MessageInfo;
using StreamMasterInfo = FileWriter::Status::StreamMasterInfo;

constexpr int NumMessages{10000};

double RandomGaussian() {
  static std::default_random_engine Generator;
  static std::normal_distribution<double> Normal(0.0, 1.0);
  return Normal(Generator);
}

TEST(MessageInfo, ZeroInitialisation) {
  MessageInfo MsgInfo;
  ASSERT_DOUBLE_EQ(MsgInfo.getMessages().first, 0.0);
  ASSERT_DOUBLE_EQ(MsgInfo.getMessages().second, 0.0);
  ASSERT_DOUBLE_EQ(MsgInfo.getMbytes().first, 0.0);
  ASSERT_DOUBLE_EQ(MsgInfo.getMbytes().second, 0.0);
  ASSERT_DOUBLE_EQ(MsgInfo.getErrors(), 0.0);
}

TEST(MessageInfo, AddOneMessage) {
  MessageInfo MsgInfo;
  const double NewMessageBytes{1024};
  MsgInfo.message(NewMessageBytes);

  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().first, 1.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().second, 1.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes().first, NewMessageBytes * 1e-6);
  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes().second,
                   NewMessageBytes * NewMessageBytes * 1e-12);
  EXPECT_DOUBLE_EQ(MsgInfo.getErrors(), 0.0);
}

TEST(MessageInfo, AddOneError) {
  MessageInfo MsgInfo;
  MsgInfo.error();

  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().first, 0.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().second, 0.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes().first, 0.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMbytes().second, 0.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getErrors(), 1.0);
}

TEST(MessageInfo, AddMessages) {
  MessageInfo MsgInfo;

  double accum{0.0}, accum2{0.0};
  for (int i = 0; i < NumMessages; ++i) {
    auto new_message_bytes = std::fabs(RandomGaussian());
    accum += new_message_bytes;
    accum2 += new_message_bytes * new_message_bytes;
    MsgInfo.message(new_message_bytes);
  }

  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().first, NumMessages);
  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().second, NumMessages);
  EXPECT_NEAR(MsgInfo.getMbytes().first, accum * 1e-6, 1e-6);
  EXPECT_NEAR(MsgInfo.getMbytes().second, accum2 * 1e-12, 1e-12);
  EXPECT_DOUBLE_EQ(MsgInfo.getErrors(), 0.0);
}

TEST(MessageInfo, ReduceMessageInfoEmpty) {

  std::vector<MessageInfo> MsgInfoVec(10);
  MessageInfo MsgInfo;
  for (auto &Elem : MsgInfoVec) {
    MsgInfo += Elem;
  }

  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().first, 0.0);
  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().second, 0.0);
  EXPECT_NEAR(MsgInfo.getMbytes().first, 0.0, 1e-6);
  EXPECT_NEAR(MsgInfo.getMbytes().second, 0.0, 1e-12);
  EXPECT_DOUBLE_EQ(MsgInfo.getErrors(), 0.0);
}

TEST(MessageInfo, ReduceMessageInfo) {
  std::vector<MessageInfo> MsgInfoVec(10);

  double TotalMessages{0.0}, TotalMessages2{0.0};
  double TotalSize{0.0}, TotalSize2{0.0};
  for (auto &Elem : MsgInfoVec) {
    for (int i = 0; i < 10; ++i) {
      Elem.message(std::fabs(RandomGaussian()));
    }
    TotalMessages += Elem.getMessages().first;
    TotalMessages2 += Elem.getMessages().second;
    TotalSize += Elem.getMbytes().first;
    TotalSize2 += Elem.getMbytes().second;
  }

  MessageInfo MsgInfo;
  for (auto &Elem : MsgInfoVec) {
    MsgInfo += Elem;
  }

  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().first, TotalMessages);
  EXPECT_DOUBLE_EQ(MsgInfo.getMessages().second, TotalMessages2);
  EXPECT_NEAR(MsgInfo.getMbytes().first, TotalSize, 1e-6);
  EXPECT_NEAR(MsgInfo.getMbytes().second, TotalSize2, 1e-12);
  EXPECT_DOUBLE_EQ(MsgInfo.getErrors(), 0.0);
}

TEST(MessageInfo, CopyMessageInfo) {
  MessageInfo MsgInfo;
  for (int i = 0; i < NumMessages; ++i) {
    MsgInfo.message(std::fabs(RandomGaussian()));
  }
  for (int i = 0; i < 10; ++i) {
    MsgInfo.error();
  }
  MessageInfo NewMsgInfo;
  NewMsgInfo = MsgInfo;

  EXPECT_EQ(NewMsgInfo.getMessages().first, MsgInfo.getMessages().first);
  EXPECT_EQ(NewMsgInfo.getMessages().second, MsgInfo.getMessages().second);
  EXPECT_EQ(NewMsgInfo.getMbytes().first, MsgInfo.getMbytes().first);
  EXPECT_EQ(NewMsgInfo.getMbytes().second, MsgInfo.getMbytes().second);
  EXPECT_EQ(NewMsgInfo.getErrors(), MsgInfo.getErrors());
}

TEST(StreamMasterInfo, InitializeEmpty) {
  StreamMasterInfo Info;
  auto &Value = Info.info();
  EXPECT_TRUE(Value.size() == 0);
}

TEST(StreamMasterInfo, AddOneInfo) {
  StreamMasterInfo Info;

  // other is required because MessageInfo::add() resets s
  MessageInfo MsgInfo, Other;
  for (int i = 0; i < NumMessages; ++i) {
    auto msg = std::fabs(RandomGaussian());
    MsgInfo.message(msg);
    Other.message(msg);
  }
  for (int i = 0; i < 10; ++i) {
    MsgInfo.error();
    Other.error();
  }
  Info.add("topic", MsgInfo);

  auto &Value = Info.info();
  EXPECT_TRUE(Value.size() == 1);

  EXPECT_EQ(Value["topic"].getMessages().first, Other.getMessages().first);
  EXPECT_EQ(Value["topic"].getMessages().second, Other.getMessages().second);
  EXPECT_EQ(Value["topic"].getMbytes().first, Other.getMbytes().first);
  EXPECT_EQ(Value["topic"].getMbytes().second, Other.getMbytes().second);
  EXPECT_EQ(Value["topic"].getErrors(), Other.getErrors());
}

TEST(StreamMasterInfo, AddMultipleInfos) {
  StreamMasterInfo Info;
  const std::vector<std::string> Topics{"first", "second", "third"};

  for (auto &t : Topics) {
    MessageInfo MsgInfo;
    Info.add(t, MsgInfo);
  }

  auto &Value = Info.info();
  EXPECT_TRUE(Value.size() == Topics.size());
}

TEST(StreamMasterInfo, AddAccumulateAllInfos) {
  StreamMasterInfo Info;
  const std::vector<std::string> Topics{"first", "second", "third"};

  double TotalMessages{0.0}, TotalMessages2{0.0};
  double TotalSize{0.0}, TotalSize2{0.0};
  double TotalErrors{0.0};

  for (auto &t : Topics) {
    MessageInfo MsgInfo;
    for (int i = 0; i < 10; ++i) {
      auto MessageSize = std::fabs(RandomGaussian());
      MsgInfo.message(MessageSize);

      TotalMessages += 1.0;
      TotalMessages2 += 1.0;
      TotalSize += MessageSize * 1e-6;
      TotalSize2 += MessageSize * MessageSize * 1e-12;
    }
    for (int i = 0; i < 10; ++i) {
      MsgInfo.error();
      TotalErrors += 1.0;
    }
    Info.add(t, MsgInfo);
  }

  auto &Value = Info.getTotal();
  EXPECT_DOUBLE_EQ(Value.getMessages().first, TotalMessages);
  EXPECT_DOUBLE_EQ(Value.getMessages().second, TotalMessages2);
  EXPECT_NEAR(Value.getMbytes().first, TotalSize, 1e-6);
  EXPECT_NEAR(Value.getMbytes().second, TotalSize2, 1e-12);
  EXPECT_DOUBLE_EQ(Value.getErrors(), TotalErrors);
}

TEST(MessageInfo, ComputeDerivedQuantities) {
  const std::vector<double> MessagesSize{1.0, 2.0, 3.0, 4.0, 5.0};
  MessageInfo MsgInfo;

  for (auto &m : MessagesSize) {
    MsgInfo.message(m * 1e6);
  }
  std::chrono::milliseconds Duration(1000);

  auto Size = FileWriter::Status::messageSize(MsgInfo);
  auto Frequency = FileWriter::Status::messageFrequency(MsgInfo, Duration);
  auto Throughput = FileWriter::Status::messageThroughput(MsgInfo, Duration);
  EXPECT_DOUBLE_EQ(Size.first, 3.0);
  EXPECT_NEAR(Size.second, 1.5811388300841898, 10e-3); // unbiased
  EXPECT_NEAR(Frequency.first, 1e3 * MessagesSize.size() / Duration.count(),
              10e-3);
  EXPECT_NEAR(Throughput.first, 1e3 * std::accumulate(MessagesSize.begin(),
                                                      MessagesSize.end(), 0.0) /
                                    Duration.count(),
              10e-3);
}

TEST(MessageInfo, DerivedQuantitiesNullDivider) {
  const std::vector<double> MessagesSize{1.0, 2.0, 3.0, 4.0, 5.0};
  MessageInfo MsgInfo;

  for (auto &m : MessagesSize) {
    MsgInfo.message(m * 1e6);
  }
  std::chrono::milliseconds Duration(0);

  auto Frequency = FileWriter::Status::messageFrequency(MsgInfo, Duration);
  auto throughput = FileWriter::Status::messageThroughput(MsgInfo, Duration);
  EXPECT_DOUBLE_EQ(0.0, Frequency.first);
  EXPECT_DOUBLE_EQ(0.0, Frequency.second);
  EXPECT_DOUBLE_EQ(0.0, throughput.first);
  EXPECT_DOUBLE_EQ(0.0, throughput.second);
}
