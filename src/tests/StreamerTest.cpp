#include "Streamer.h"
#include <gtest/gtest.h>
#include <librdkafka/rdkafka.h>
#include <trompeloeil.hpp>

using trompeloeil::_;

namespace FileWriter {
std::unique_ptr<KafkaW::ConsumerMessage>
generateKafkaMsg(unsigned char const *DataPtr, size_t const Size) {
  return std::make_unique<KafkaW::ConsumerMessage>(DataPtr, Size,
                                                   std::function<void()>());
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

// Disabled for now as there is a problem with the Consumer that requires a
// re-writer of it
//  TEST_F(StreamerTest, CreateConsumerSuccess) {
//    StreamerOptions SomeOptions;
//    SomeOptions.Settings.Address = "127.0.0.1:9999";
//    SomeOptions.Settings.ConfigurationStrings["group.id"] = "TestGroup";
//    std::string TopicName{"SomeName"};
//    std::pair<Status::StreamerError,ConsumerPtr> Result =
//    createConsumer(TopicName, SomeOptions);
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
  void SetUp() override { Settings.Address = "127.0.0.1:1"; }
  KafkaW::BrokerSettings Settings;
};

class ConsumerEmptyStandIn
    : public trompeloeil::mock_interface<KafkaW::ConsumerInterface> {
public:
  ConsumerEmptyStandIn(KafkaW::BrokerSettings const &Settings){};
  MAKE_MOCK0(poll, std::unique_ptr<KafkaW::ConsumerMessage>(), override);
  IMPLEMENT_MOCK1(addTopic);
  IMPLEMENT_MOCK2(addTopicAtTimestamp);
  IMPLEMENT_MOCK0(dumpCurrentSubscription);
  IMPLEMENT_MOCK1(topicPresent);
  IMPLEMENT_MOCK1(queryTopicPartitions);
};

TEST_F(StreamerProcessTest, CreationNotYetDone) {

  StreamerStandIn TestStreamer;
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(Settings);
  ALLOW_CALL(*EmptyPollerConsumer, addTopic(_));
  // ALLOW_CALL(*EmptyPollerConsumer,addTopicAtTimestamp(_,_)).RETURN(true);
  // ALLOW_CALL(*EmptyPollerConsumer,dumpCurrentSubscription()).RETURN(true);
  ALLOW_CALL(*EmptyPollerConsumer, topicPresent(_)).RETURN(true);
  //  ALLOW_CALL(*EmptyPollerConsumer,queryTopicPartitions(_)).RETURN(true);
  REQUIRE_CALL(*EmptyPollerConsumer, poll()).TIMES(0);
  TestStreamer.ConsumerCreated.get();
  TestStreamer.ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
}
//
// TEST_F(StreamerProcessTest, InvalidFuture) {
//  StreamerStandIn TestStreamer;
//  TestStreamer.ConsumerCreated =
//      std::future<std::pair<Status::StreamerStatus, ConsumerPtr>>();
//  DemuxTopic Demuxer("SomeTopicName");
//  EXPECT_THROW(TestStreamer.pollAndProcess(Demuxer), std::runtime_error);
//}
//
// TEST_F(StreamerProcessTest, BadConsumerCreation) {
//  StreamerStandIn TestStreamer;
//  TestStreamer.ConsumerCreated = std::async(std::launch::async, []() {
//    return std::pair<Status::StreamerStatus, ConsumerPtr>{
//        Status::StreamerStatus::CONFIGURATION_ERROR, nullptr};
//  });
//  DemuxTopic Demuxer("SomeTopicName");
//  EXPECT_THROW(TestStreamer.pollAndProcess(Demuxer), std::runtime_error);
//}
//
// TEST_F(StreamerProcessTest, EmptyPoll) {
//  StreamerStandIn TestStreamer;
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(
//          std::make_unique<KafkaW::ConsumerMessage>(KafkaW::PollStatus::Empty))
//      .TIMES(1);
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxTopic Demuxer("SomeTopicName");
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
//}
//
// TEST_F(StreamerProcessTest, EndOfPartition) {
//  StreamerStandIn TestStreamer;
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(
//          std::make_unique<KafkaW::ConsumerMessage>(KafkaW::PollStatus::EOP))
//      .TIMES(1);
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxTopic Demuxer("SomeTopicName");
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
//}
//
// TEST_F(StreamerProcessTest, PollingError) {
//  StreamerStandIn TestStreamer;
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(
//          std::make_unique<KafkaW::ConsumerMessage>(KafkaW::PollStatus::Err))
//      .TIMES(1);
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxTopic Demuxer("SomeTopicName");
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::ERR);
//}
//
// TEST_F(StreamerProcessTest, InvalidMessage) {
//  std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
//      FlatbufferReaderRegistry::getReaders();
//  Readers.clear();
//  unsigned char DataBuffer[]{"0000test"};
//  std::string ReaderKey{"test"};
//  StreamerStandIn TestStreamer;
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(generateKafkaMsg(DataBuffer, sizeof(DataBuffer)))
//      .TIMES(1);
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxTopic Demuxer("SomeTopicName");
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::ERR);
//}
//
// class StreamerNoTimestampTestDummyReader : public
// FileWriter::FlatbufferReader {
// public:
//  bool verify(FlatbufferMessage const &Message) const override { return true;
//  }
//  std::string source_name(FlatbufferMessage const &Message) const override {
//    return std::string("SomeRandomSourceName");
//  }
//  std::uint64_t timestamp(FlatbufferMessage const &Message) const override {
//    return 0;
//  }
//};
//
// class StreamerTestDummyReader : public FileWriter::FlatbufferReader {
// public:
//  bool verify(FlatbufferMessage const &Message) const override { return true;
//  }
//  std::string source_name(FlatbufferMessage const &Message) const override {
//    return std::string("SomeRandomSourceName");
//  }
//  std::uint64_t timestamp(FlatbufferMessage const &Message) const override {
//    return 1;
//  }
//};
//
// TEST_F(StreamerProcessTest, UnknownSourceName) {
//  std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
//      FlatbufferReaderRegistry::getReaders();
//  Readers.clear();
//  std::string ReaderKey{"test"};
//
//  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
//      ReaderKey);
//  unsigned char DataBuffer[]{"0000test"};
//
//  StreamerStandIn TestStreamer;
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(std::unique_ptr<KafkaW::ConsumerMessage>(
//          generateKafkaMsg(DataBuffer, sizeof(DataBuffer))))
//      .TIMES(1);
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxTopic Demuxer("SomeTopicName");
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
//}
//
// class WriterModuleStandIn : public HDFWriterModule {
// public:
//  MAKE_MOCK2(parse_config, void(std::string const &, std::string const &),
//             override);
//  MAKE_MOCK2(init_hdf, InitResult(hdf5::node::Group &, std::string const &),
//             override);
//  MAKE_MOCK1(reopen, InitResult(hdf5::node::Group &), override);
//  MAKE_MOCK1(write, WriteResult(FlatbufferMessage const &), override);
//  MAKE_MOCK0(flush, int32_t(), override);
//  MAKE_MOCK0(close, int32_t(), override);
//  MAKE_MOCK3(enable_cq, void(CollectiveQueue *, HDFIDStore *, int), override);
//};
//
// class StreamerProcessTimingTest : public ::testing::Test {
// protected:
//  void SetUp() override {
//    Settings.Address = "127.0.0.1:1";
//    std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
//        FlatbufferReaderRegistry::getReaders();
//    Readers.clear();
//  }
//  std::string ReaderKey{"test"};
//  std::string DataBuffer{"0000test"};
//  std::string SourceName{"SomeRandomSourceName"};
//  KafkaW::BrokerSettings Settings;
//  StreamerStandIn TestStreamer;
//};
//
// TEST_F(StreamerProcessTimingTest,
//       pollAndProcessReturnsErrIfMessageHasNoTimestamp) {
//  FlatbufferReaderRegistry::Registrar<StreamerNoTimestampTestDummyReader>
//      RegisterIt(ReaderKey);
//  TestStreamer.Options.StartTimestamp = std::chrono::milliseconds{1};
//  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
//      .RETURN(0);
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
//      .RETURN(0);
//  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
//  std::unordered_map<std::string, Source> SourceList;
//  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
//  SourceList.insert(std::move(TempPair));
//  TestStreamer.setSources(SourceList);
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(std::unique_ptr<KafkaW::ConsumerMessage>(generateKafkaMsg(
//          reinterpret_cast<const unsigned char *>(DataBuffer.c_str()),
//          DataBuffer.size())))
//      .TIMES(1);
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxTopic Demuxer("SomeTopicName");
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::ERR);
//}
//
// TEST_F(StreamerProcessTimingTest, MessageBeforeStartTimestamp) {
//  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
//      ReaderKey);
//  TestStreamer.Options.StartTimestamp = std::chrono::milliseconds{1};
//  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
//      .RETURN(0);
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
//      .RETURN(0);
//  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
//  std::unordered_map<std::string, Source> SourceList;
//  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
//  SourceList.insert(std::move(TempPair));
//  TestStreamer.setSources(SourceList);
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(generateKafkaMsg(
//          reinterpret_cast<const unsigned char *>(DataBuffer.c_str()),
//          DataBuffer.size()))
//      .TIMES(1);
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxTopic Demuxer("SomeTopicName");
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
//}
//
// class StreamerHighTimestampTestDummyReader
//    : public FileWriter::FlatbufferReader {
// public:
//  bool verify(FlatbufferMessage const &Message) const override { return true;
//  }
//  std::string source_name(FlatbufferMessage const &Message) const override {
//    return std::string("SomeRandomSourceName");
//  }
//  std::uint64_t timestamp(FlatbufferMessage const &Message) const override {
//    return 5000000;
//  }
//};
//
// TEST_F(StreamerProcessTimingTest, MessageAfterStopTimestamp) {
//  FlatbufferReaderRegistry::Registrar<StreamerHighTimestampTestDummyReader>
//      RegisterIt(ReaderKey);
//  TestStreamer.Options.StopTimestamp = std::chrono::milliseconds{1};
//  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
//      .RETURN(0);
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
//      .RETURN(0);
//  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
//  std::unordered_map<std::string, Source> SourceList;
//  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
//  SourceList.insert(std::move(TempPair));
//  TestStreamer.setSources(SourceList);
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(generateKafkaMsg(
//          reinterpret_cast<const unsigned char *>(DataBuffer.c_str()),
//          DataBuffer.size()))
//      .TIMES(1);
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxTopic Demuxer("SomeTopicName");
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::STOP);
//}
//
// class DemuxerStandIn : public DemuxTopic {
// public:
//  DemuxerStandIn(std::string Topic) : DemuxTopic(Topic) {}
//  MAKE_MOCK1(process_message, ProcessMessageResult(FlatbufferMessage const &),
//             override);
//};
//
// TEST_F(StreamerProcessTimingTest, MessageTimeout) {
//  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
//      ReaderKey);
//  TestStreamer.Options.StopTimestamp = std::chrono::milliseconds{1};
//  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
//      .RETURN(0);
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
//      .RETURN(0);
//  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
//  std::unordered_map<std::string, Source> SourceList;
//  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
//  SourceList.insert(std::move(TempPair));
//  TestStreamer.setSources(SourceList);
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  int CallCounter{0};
//  auto PollResult = [this, &CallCounter]() {
//    CallCounter++;
//    if (CallCounter == 1) {
//      return generateKafkaMsg(
//          reinterpret_cast<const unsigned char *>(DataBuffer.c_str()),
//          DataBuffer.size());
//    }
//    return std::make_unique<KafkaW::ConsumerMessage>(KafkaW::PollStatus::EOP);
//  };
//  REQUIRE_CALL(*EmptyPollerConsumer, poll()).RETURN(PollResult()).TIMES(2);
//
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxerStandIn Demuxer("SomeTopicName");
//  REQUIRE_CALL(Demuxer, process_message(_))
//      .RETURN(ProcessMessageResult::OK)
//      .TIMES(1);
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
//  std::this_thread::sleep_for(std::chrono::milliseconds(5));
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::STOP);
//}
//
// TEST_F(StreamerProcessTimingTest, EmptyMessageAfterStop) {
//  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
//      ReaderKey);
//
//  TestStreamer.Options.StopTimestamp = std::chrono::milliseconds{5};
//  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
//      .RETURN(0);
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
//      .RETURN(0);
//  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
//  std::unordered_map<std::string, Source> SourceList;
//  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
//  SourceList.insert(std::move(TempPair));
//  TestStreamer.setSources(SourceList);
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(
//          std::make_unique<KafkaW::ConsumerMessage>(KafkaW::PollStatus::EOP))
//      .TIMES(1);
//
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxerStandIn Demuxer("SomeTopicName");
//  REQUIRE_CALL(Demuxer, process_message(_)).TIMES(0);
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::STOP);
//}
//
// TEST_F(StreamerProcessTimingTest, EmptyMessageBeforeStop) {
//  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
//      ReaderKey);
//  auto Now = std::chrono::system_clock::now();
//  auto Then = std::chrono::duration_cast<std::chrono::milliseconds>(
//                  Now.time_since_epoch()) +
//              std::chrono::milliseconds(12000);
//  TestStreamer.Options.StopTimestamp = Then;
//  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
//      .RETURN(0);
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
//      .RETURN(0);
//  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
//  std::unordered_map<std::string, Source> SourceList;
//  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
//  SourceList.insert(std::move(TempPair));
//  TestStreamer.setSources(SourceList);
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(
//          std::make_unique<KafkaW::ConsumerMessage>(KafkaW::PollStatus::EOP))
//      .TIMES(1);
//
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxerStandIn Demuxer("SomeTopicName");
//  REQUIRE_CALL(Demuxer, process_message(_)).TIMES(0);
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
//}
//
//// According to Michele, that functionality is currently broken and needs to
//// be
//// revisited.
//// See issue #360
// TEST_F(StreamerProcessTimingTest, DISABLED_EmptyMessageSlightlyAfterStop) {
//  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
//
//      ReaderKey);
//  namespace c = std::chrono;
//  auto Now = c::duration_cast<c::milliseconds>(
//      c::system_clock::now().time_since_epoch());
//  TestStreamer.Options.StopTimestamp = Now;
//  TestStreamer.Options.AfterStopTime = c::milliseconds(5000);
//  std::this_thread::sleep_for(c::milliseconds(5));
//  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
//      .RETURN(0);
//  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
//      .RETURN(0);
//  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
//  std::unordered_map<std::string, Source> SourceList;
//  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
//  SourceList.insert(std::move(TempPair));
//  TestStreamer.setSources(SourceList);
//  ConsumerEmptyStandIn *EmptyPollerConsumer =
//      new ConsumerEmptyStandIn(Settings);
//  REQUIRE_CALL(*EmptyPollerConsumer, poll())
//      .RETURN(
//          std::make_unique<KafkaW::ConsumerMessage>(KafkaW::PollStatus::EOP))
//      .TIMES(1);
//
//  TestStreamer.ConsumerCreated =
//      std::async(std::launch::async, [&EmptyPollerConsumer]() {
//        return std::pair<Status::StreamerStatus, ConsumerPtr>{
//            Status::StreamerStatus::OK, EmptyPollerConsumer};
//      });
//  DemuxerStandIn Demuxer("SomeTopicName");
//  REQUIRE_CALL(Demuxer, process_message(_)).TIMES(0);
//  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
//}
} // namespace FileWriter
