#include "KafkaWMocks.h"

#include <Msg.h>
#include <gtest/gtest.h>

using namespace KafkaW;
using ::testing::Exactly;
using ::testing::Return;
using ::testing::_;
using ::testing::SetArgPointee;

class ConsumerStandIn : public Consumer {
public:
  ConsumerStandIn(BrokerSettings &Settings) : Consumer(Settings) {}
  using Consumer::KafkaConsumer;
};

class ConsumerTests : public ::testing::Test {
protected:
  void SetUp() override {
    Consumer = new MockKafkaConsumer;
    StandIn.KafkaConsumer.reset(Consumer);
  }

  MockKafkaConsumer *Consumer;
  BrokerSettings Settings;
  ConsumerStandIn StandIn{Settings};

  std::unique_ptr<std::pair<PollStatus, FileWriter::Msg>> validMessageToPoll() {
    PollStatus Status;
    FileWriter::Msg KafkaMessage;

    std::pair<PollStatus, FileWriter::Msg> NewPair(Status,
                                                   std::move(KafkaMessage));
    std::unique_ptr<std::pair<PollStatus, FileWriter::Msg>> DataToReturn;
    DataToReturn = std::make_unique<std::pair<PollStatus, FileWriter::Msg>>(
        std::move(NewPair));
    return DataToReturn;
  }
};

TEST_F(ConsumerTests, pollReturnsConsumerMessageWithMessagePollStatus) {
  MockMessage *Message = new MockMessage;
  EXPECT_CALL(*Message, len()).Times(Exactly(1)).WillOnce(Return(1));
  EXPECT_CALL(*Message, err())
      .Times(Exactly(1))
      .WillOnce(Return(RdKafka::ErrorCode::ERR_NO_ERROR));
  std::string Payload{"test"};
  EXPECT_CALL(*Message, payload())
      .Times(Exactly(1))
      .WillOnce(Return(reinterpret_cast<void *>(&Payload)));

  EXPECT_CALL(*Consumer, consume(_))
      .Times(Exactly(1))
      .WillOnce(Return(Message));
  EXPECT_CALL(*Consumer, close()).Times(Exactly(1));

  auto ConsumedMessage = StandIn.poll();
  ASSERT_EQ(ConsumedMessage->first, PollStatus::Message);
}