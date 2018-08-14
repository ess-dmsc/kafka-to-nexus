#include "Streamer.h"
#include <gtest/gtest.h>

namespace FileWriter {
class StreamerInitTest : public ::testing::Test {
public:
};

// make sure that a topic exists/not exists
TEST_F(StreamerInitTest, Success) {
  EXPECT_NO_THROW(Streamer("broker", "topic", StreamerOptions()));
}
  
  TEST_F(StreamerInitTest, NoBroker) {
    EXPECT_THROW(Streamer("", "topic", StreamerOptions()), std::runtime_error);
  }
  
  TEST_F(StreamerInitTest, NoTopic) {
    EXPECT_THROW(Streamer("broker", "", StreamerOptions()), std::runtime_error);
  }
  
  // Disabled for now as there is a problem with the Consumer that requires a re-writer of it
//  TEST_F(StreamerTest, CreateConsumerSuccess) {
//    StreamerOptions SomeOptions;
//    SomeOptions.Settings.Address = "127.0.0.1:9999";
//    SomeOptions.Settings.ConfigurationStrings["group.id"] = "TestGroup";
//    std::string TopicName{"SomeName"};
//    std::pair<Status::StreamerError,ConsumerPtr> Result = createConsumer(TopicName, SomeOptions);
//    EXPECT_TRUE(Result.first.connectionOK());
//    EXPECT_NE(Result.second.get(), nullptr);
//  }
  
  class StreamerStandIn : public Streamer {
  public:
    StreamerStandIn() : Streamer("SomeBroker", "SomeTopic", StreamerOptions()) {}
    using Streamer::ConsumerCreated;
  };
  class StreamerProcessTest : public ::testing::Test {
  public:
  };
  
  TEST_F(StreamerProcessTest, CreationNotYetDone) {
    StreamerStandIn TestStreamer;
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [](){
      std::this_thread::sleep_for(std::chrono::milliseconds(2500));
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::OK, nullptr};
    });
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
  }
  
  TEST_F(StreamerProcessTest, InvalidFuture) {
    StreamerStandIn TestStreamer;
    TestStreamer.ConsumerCreated = std::future<std::pair<Status::StreamerStatus,ConsumerPtr>>();
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_THROW(TestStreamer.pollAndProcess(Demuxer), std::runtime_error);
  }
  
  TEST_F(StreamerProcessTest, BadConsumerCreation) {
    StreamerStandIn TestStreamer;
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [](){
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::CONFIGURATION_ERROR, nullptr};
    });
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_THROW(TestStreamer.pollAndProcess(Demuxer), std::runtime_error);
  }
  
  class ConsumerEmptyStandIn : public KafkaW::Consumer {
  public:
    ConsumerEmptyStandIn() : KafkaW::Consumer(KafkaW::BrokerSettings()) {};
    KafkaW::PollStatus poll() override {
      return KafkaW::PollStatus::Empty();
    }
  };
  
  TEST_F(StreamerProcessTest, EmptyPoll) {
    StreamerStandIn TestStreamer;
    KafkaW::Consumer *EmptyPollerConsumer = new ConsumerEmptyStandIn();
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [&EmptyPollerConsumer](){
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::CONFIGURATION_ERROR, EmptyPollerConsumer};
    });
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
  }
  
} //namespace FileWriter
