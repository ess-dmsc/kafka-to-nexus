#include "Streamer.h"
#include <gtest/gtest.h>
#include <trompeloeil.hpp>
#include <librdkafka/rdkafka.h>

using trompeloeil::_;

namespace FileWriter {
  std::unique_ptr<KafkaW::Msg> generateKafkaMsg(unsigned char const *DataPtr, size_t const Size) {
    return std::make_unique<KafkaW::Msg>(DataPtr, Size, std::function<void()>());
  }
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
    using Streamer::Options;
  };
  class StreamerProcessTest : public ::testing::Test {
  protected:
    void SetUp() override {
      Settings.Address = "127.0.0.1:1";
    }
    KafkaW::BrokerSettings Settings;
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
    ConsumerEmptyStandIn(KafkaW::BrokerSettings const &Settings) : KafkaW::Consumer(Settings) {};
    MAKE_MOCK0(poll, KafkaW::PollStatus(), override);
  };
  
  TEST_F(StreamerProcessTest, EmptyPoll) {
    StreamerStandIn TestStreamer;
    ConsumerEmptyStandIn *EmptyPollerConsumer = new ConsumerEmptyStandIn(Settings);
    REQUIRE_CALL(*EmptyPollerConsumer, poll()).RETURN(KafkaW::PollStatus::Empty()).TIMES(1);
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [&EmptyPollerConsumer](){
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::OK, EmptyPollerConsumer};
    });
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
  }
  
  TEST_F(StreamerProcessTest, EndOfPartition) {
    StreamerStandIn TestStreamer;
    ConsumerEmptyStandIn *EmptyPollerConsumer = new ConsumerEmptyStandIn(Settings);
    REQUIRE_CALL(*EmptyPollerConsumer, poll()).RETURN(KafkaW::PollStatus::EOP()).TIMES(1);
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [&EmptyPollerConsumer](){
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::OK, EmptyPollerConsumer};
    });
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
  }
  
  TEST_F(StreamerProcessTest, PollingError) {
    StreamerStandIn TestStreamer;
    ConsumerEmptyStandIn *EmptyPollerConsumer = new ConsumerEmptyStandIn(Settings);
    REQUIRE_CALL(*EmptyPollerConsumer, poll()).RETURN(KafkaW::PollStatus::Err()).TIMES(1);
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [&EmptyPollerConsumer](){
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::OK, EmptyPollerConsumer};
    });
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::ERR);
  }
  
  TEST_F(StreamerProcessTest, InvalidMessage) {
    std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
    FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    unsigned char DataBuffer[]{"0000test"};
    std::string ReaderKey{"test"};
    StreamerStandIn TestStreamer;
    ConsumerEmptyStandIn *EmptyPollerConsumer = new ConsumerEmptyStandIn(Settings);
    REQUIRE_CALL(*EmptyPollerConsumer, poll()).RETURN(KafkaW::PollStatus::newWithMsg(std::unique_ptr<KafkaW::Msg>(generateKafkaMsg(DataBuffer, sizeof(DataBuffer))))).TIMES(1);
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [&EmptyPollerConsumer](){
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::OK, EmptyPollerConsumer};
    });
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::ERR);
  }
  
  class StreamerTestDummyReader : public FileWriter::FlatbufferReader {
  public:
    bool verify(FlatbufferMessage const &Message) const override { return true; }
    std::string source_name(FlatbufferMessage const &Message) const override {
      return std::string("SomeRandomSourceName");
    }
    std::uint64_t timestamp(FlatbufferMessage const &Message) const override { return 0; }
  };
  
  TEST_F(StreamerProcessTest, UnknownSourceName) {
    std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
    FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    std::string ReaderKey{"test"};
    
    FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(ReaderKey);
    unsigned char DataBuffer[]{"0000test"};
    
    StreamerStandIn TestStreamer;
    ConsumerEmptyStandIn *EmptyPollerConsumer = new ConsumerEmptyStandIn(Settings);
    REQUIRE_CALL(*EmptyPollerConsumer, poll()).RETURN(KafkaW::PollStatus::newWithMsg(std::unique_ptr<KafkaW::Msg>(generateKafkaMsg(DataBuffer, sizeof(DataBuffer))))).TIMES(1);
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [&EmptyPollerConsumer](){
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::OK, EmptyPollerConsumer};
    });
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
  }
  
  class WriterModuleStandIn : public HDFWriterModule {
  public:
    MAKE_MOCK2(parse_config, void(std::string const&, std::string const&), override);
    MAKE_MOCK2(init_hdf, InitResult(hdf5::node::Group&, std::string const&), override);
    MAKE_MOCK1(reopen, InitResult(hdf5::node::Group&), override);
    MAKE_MOCK1(write, WriteResult(FlatbufferMessage const&), override);
    MAKE_MOCK0(flush, int32_t(), override);
    MAKE_MOCK0(close, int32_t(), override);
    MAKE_MOCK3(enable_cq, void(CollectiveQueue*, HDFIDStore*, int), override);
  };
  
  TEST_F(StreamerProcessTest, MessageBeforeStartTimestamp) {
    std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
    FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    std::string ReaderKey{"test"};
    
    FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(ReaderKey);
    unsigned char DataBuffer[]{"0000test"};
    
    StreamerStandIn TestStreamer;
    TestStreamer.Options.StartTimestamp = std::chrono::milliseconds{1};
    std::string SourceName{"SomeRandomSourceName"};
    HDFWriterModule::ptr Writer(new WriterModuleStandIn());
    FileWriter::Source TestSource(SourceName, std::move(Writer));
    std::unordered_map<std::string, Source> SourceList;
    std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
    SourceList.insert(std::move(TempPair));
    TestStreamer.setSources(SourceList);
    ConsumerEmptyStandIn *EmptyPollerConsumer = new ConsumerEmptyStandIn(Settings);
    REQUIRE_CALL(*EmptyPollerConsumer, poll()).RETURN(KafkaW::PollStatus::newWithMsg(std::unique_ptr<KafkaW::Msg>(generateKafkaMsg(DataBuffer, sizeof(DataBuffer))))).TIMES(1);
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [&EmptyPollerConsumer](){
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::OK, EmptyPollerConsumer};
    });
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
  }
  
  class StreamerHighTimestampTestDummyReader : public FileWriter::FlatbufferReader {
  public:
    bool verify(FlatbufferMessage const &Message) const override { return true; }
    std::string source_name(FlatbufferMessage const &Message) const override {
      return std::string("SomeRandomSourceName");
    }
    std::uint64_t timestamp(FlatbufferMessage const &Message) const override { return 5000000; }
  };
  
  TEST_F(StreamerProcessTest, MessageAfterStopTimestamp) {
    std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
    FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    std::string ReaderKey{"test"};
    
    FlatbufferReaderRegistry::Registrar<StreamerHighTimestampTestDummyReader> RegisterIt(ReaderKey);
    unsigned char DataBuffer[]{"0000test"};
    
    StreamerStandIn TestStreamer;
    TestStreamer.Options.StopTimestamp = std::chrono::milliseconds{1};
    std::string SourceName{"SomeRandomSourceName"};
    HDFWriterModule::ptr Writer(new WriterModuleStandIn());
    FileWriter::Source TestSource(SourceName, std::move(Writer));
    std::unordered_map<std::string, Source> SourceList;
    std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
    SourceList.insert(std::move(TempPair));
    TestStreamer.setSources(SourceList);
    ConsumerEmptyStandIn *EmptyPollerConsumer = new ConsumerEmptyStandIn(Settings);
    REQUIRE_CALL(*EmptyPollerConsumer, poll()).RETURN(KafkaW::PollStatus::newWithMsg(std::unique_ptr<KafkaW::Msg>(generateKafkaMsg(DataBuffer, sizeof(DataBuffer))))).TIMES(1);
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [&EmptyPollerConsumer](){
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::OK, EmptyPollerConsumer};
    });
    DemuxTopic Demuxer("SomeTopicName");
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::STOP);
  }
  
  class DemuxerStandIn : public DemuxTopic {
  public:
    DemuxerStandIn(std::string Topic) : DemuxTopic(Topic) {}
    MAKE_MOCK1(process_message, ProcessMessageResult(FlatbufferMessage const&), override);
  };
  
  TEST_F(StreamerProcessTest, MessageTimeout) {
    std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
    FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    std::string ReaderKey{"test"};
    
    FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(ReaderKey);
    unsigned char DataBuffer[]{"0000test"};
    
    StreamerStandIn TestStreamer;
    TestStreamer.Options.StopTimestamp = std::chrono::milliseconds{1};
    TestStreamer.Options.ConsumerTimeout = std::chrono::milliseconds{1};
    std::string SourceName{"SomeRandomSourceName"};
    HDFWriterModule::ptr Writer(new WriterModuleStandIn());
    FileWriter::Source TestSource(SourceName, std::move(Writer));
    std::unordered_map<std::string, Source> SourceList;
    std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
    SourceList.insert(std::move(TempPair));
    TestStreamer.setSources(SourceList);
    ConsumerEmptyStandIn *EmptyPollerConsumer = new ConsumerEmptyStandIn(Settings);
    int CallCounter{0};
    auto PollResult = [&DataBuffer,&CallCounter](){
      CallCounter++;
      if (CallCounter == 1) {
      return KafkaW::PollStatus::newWithMsg(std::unique_ptr<KafkaW::Msg>(generateKafkaMsg(DataBuffer, sizeof(DataBuffer))));
      }
      return KafkaW::PollStatus::EOP();
      };
    REQUIRE_CALL(*EmptyPollerConsumer, poll()).RETURN(PollResult()).TIMES(2);
    
    TestStreamer.ConsumerCreated = std::async(std::launch::async, [&EmptyPollerConsumer](){
      return std::pair<Status::StreamerStatus,ConsumerPtr>{Status::StreamerStatus::OK, EmptyPollerConsumer};
    });
    DemuxerStandIn Demuxer("SomeTopicName");
    REQUIRE_CALL(Demuxer, process_message(_)).RETURN(ProcessMessageResult::OK).TIMES(1);
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::STOP);
  }
  
} //namespace FileWriter
