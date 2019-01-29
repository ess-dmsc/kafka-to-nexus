#include "KafkaWMocks.h"

#include <Msg.h>
#include <gtest/gtest.h>

using namespace KafkaW;
using trompeloeil::_;

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
  auto *Message = new MockMessage;
  REQUIRE_CALL(*Message, len()).TIMES(1).RETURN(1);
  REQUIRE_CALL(*Message, err())
      .TIMES(1)
      .RETURN(RdKafka::ErrorCode::ERR_NO_ERROR);
  REQUIRE_CALL(*Message, payload()).TIMES(1).RETURN(nullptr);

  REQUIRE_CALL(*Consumer, consume(_)).TIMES(1).RETURN(Message);
  REQUIRE_CALL(*Consumer, close()).TIMES(1).RETURN(RdKafka::ERR_NO_ERROR);
  // Put this in scope to call standin destructor
  {
    ConsumerStandIn Cons{Settings};
    Cons.KafkaConsumer.reset(Consumer);
    auto ConsumedMessage = Cons.poll();
    ASSERT_EQ(ConsumedMessage->first, PollStatus::Message);
  }
}