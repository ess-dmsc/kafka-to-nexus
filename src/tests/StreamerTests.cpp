#include "Streamer.h"
#include <Msg.h>
#include <chrono>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <schemas/f142/FlatbufferReader.h>
#include <trompeloeil.hpp>

namespace FileWriter {
namespace Schemas {
namespace f142 {
#include "schemas/f142_logdata_generated.h"
}
}
}

using trompeloeil::_;
using KafkaW::PollStatus;
using namespace FileWriter;

std::unique_ptr<std::pair<PollStatus, Msg>>
generateKafkaMsg(char const *DataPtr, size_t const Size) {
  FileWriter::Msg Message = FileWriter::Msg::owned(DataPtr, Size);
  Message.MetaData = FileWriter::MessageMetaData{
      std::chrono::milliseconds(0),
      RdKafka::MessageTimestamp::MessageTimestampType::
          MSG_TIMESTAMP_CREATE_TIME,
      0};
  std::pair<PollStatus, FileWriter::Msg> NewPair(PollStatus::Message,
                                                 std::move(Message));
  return std::make_unique<std::pair<PollStatus, FileWriter::Msg>>(
      std::move(NewPair));
}
std::unique_ptr<std::pair<PollStatus, Msg>>
generateEmptyKafkaMsg(PollStatus Status) {
  FileWriter::Msg KafkaMessage;
  std::pair<PollStatus, FileWriter::Msg> NewPair(Status,
                                                 std::move(KafkaMessage));
  return std::make_unique<std::pair<PollStatus, FileWriter::Msg>>(
      std::move(NewPair));
}

class StreamerInitTest : public ::testing::Test {
protected:
  void SetUp() override { Options.BrokerSettings.MetadataTimeoutMS = 10; }
  StreamerOptions Options;
};

TEST_F(StreamerInitTest, CheckTopicExists) {
  EXPECT_NO_THROW(Streamer("broker", "topic", Options));
}

TEST_F(StreamerInitTest, ThrowsIfNoBrokerProvided) {
  EXPECT_THROW(Streamer("", "topic", Options), std::runtime_error);
}

TEST_F(StreamerInitTest, ThrowsIfNoTopicProvided) {
  EXPECT_THROW(Streamer("broker", "", Options), std::runtime_error);
}

class StreamerStandIn : public Streamer {
public:
  StreamerStandIn() : Streamer("SomeBroker", "SomeTopic", StreamerOptions()) {}
  explicit StreamerStandIn(StreamerOptions Opts)
      : Streamer("SomeBroker", "SomeTopic", Opts) {}
  using Streamer::ConsumerCreated;
  using Streamer::Options;
};

class StreamerProcessTest : public ::testing::Test {
protected:
  void SetUp() override {
    BrokerSettings.Address = "127.0.0.1:1";
    Options.BrokerSettings = BrokerSettings;
  }
  KafkaW::BrokerSettings BrokerSettings;
  StreamerOptions Options;
};

class ConsumerEmptyStandIn
    : public trompeloeil::mock_interface<KafkaW::ConsumerInterface> {
public:
  explicit ConsumerEmptyStandIn(const KafkaW::BrokerSettings &Settings){};
  IMPLEMENT_MOCK1(addTopic);
  IMPLEMENT_MOCK2(addTopicAtTimestamp);
  IMPLEMENT_MOCK1(topicPresent);
  IMPLEMENT_MOCK1(queryTopicPartitions);
  IMPLEMENT_MOCK0(poll);
};

TEST_F(StreamerProcessTest, CreationNotYetDone) {
  StreamerStandIn TestStreamer(Options);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll()).TIMES(0);
  TestStreamer.ConsumerCreated.get();
  TestStreamer.ConsumerCreated =
      std::async(std::launch::async, [EmptyPollerConsumer]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
}

TEST_F(StreamerProcessTest, InvalidFuture) {
  StreamerStandIn TestStreamer(Options);
  TestStreamer.ConsumerCreated =
      std::future<std::pair<Status::StreamerStatus, ConsumerPtr>>();
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_THROW(TestStreamer.pollAndProcess(Demuxer), std::runtime_error);
}

TEST_F(StreamerProcessTest, BadConsumerCreation) {
  StreamerStandIn TestStreamer(Options);
  TestStreamer.ConsumerCreated = std::async(std::launch::async, []() {
    return std::pair<Status::StreamerStatus, ConsumerPtr>{
        Status::StreamerStatus::CONFIGURATION_ERROR, nullptr};
  });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_THROW(TestStreamer.pollAndProcess(Demuxer), std::runtime_error);
}

TEST_F(StreamerProcessTest, EmptyPoll) {
  StreamerStandIn TestStreamer(Options);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::Empty))
      .TIMES(1);
  TestStreamer.ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
}

TEST_F(StreamerProcessTest, EndOfPartition) {
  StreamerStandIn TestStreamer(Options);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);
  TestStreamer.ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
}

TEST_F(StreamerProcessTest, PollingError) {
  StreamerStandIn TestStreamer(Options);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::Error))
      .TIMES(1);
  TestStreamer.ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::ERR);
}

TEST_F(StreamerProcessTest, InvalidMessage) {
  std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
      FlatbufferReaderRegistry::getReaders();
  Readers.clear();
  char DataBuffer[]{"0000test"};
  std::string ReaderKey{"test"};

  StreamerStandIn TestStreamer(Options);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateKafkaMsg(DataBuffer, sizeof(DataBuffer)))
      .TIMES(1);
  TestStreamer.ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::ERR);
}

class StreamerNoTimestampTestDummyReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override { return true; }
  std::string source_name(FlatbufferMessage const &Message) const override {
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t timestamp(FlatbufferMessage const &Message) const override {
    return 0;
  }
};

class StreamerTestDummyReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override { return true; }
  std::string source_name(FlatbufferMessage const &Message) const override {
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t timestamp(FlatbufferMessage const &Message) const override {
    return 1;
  }
};

TEST_F(StreamerProcessTest, UnknownSourceName) {
  std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
      FlatbufferReaderRegistry::getReaders();
  Readers.clear();
  std::string ReaderKey{"test"};

  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
      ReaderKey);
  char DataBuffer[]{"0000test"};

  StreamerStandIn TestStreamer(Options);

  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);

  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateKafkaMsg(static_cast<const char *>(DataBuffer),
                               sizeof(DataBuffer)))
      .TIMES(1);
  TestStreamer.ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });

  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer.pollAndProcess(Demuxer), ProcessMessageResult::OK);
}

class WriterModuleStandIn : public HDFWriterModule {
public:
  MAKE_MOCK2(parse_config, void(std::string const &, std::string const &),
             override);
  MAKE_MOCK2(init_hdf, InitResult(hdf5::node::Group &, std::string const &),
             override);
  MAKE_MOCK1(reopen, InitResult(hdf5::node::Group &), override);
  MAKE_MOCK1(write, void(FlatbufferMessage const &), override);
  MAKE_MOCK0(flush, int32_t(), override);
  MAKE_MOCK0(close, int32_t(), override);
};

class StreamerProcessTimingTest : public ::testing::Test {
protected:
  void SetUp() override {
    BrokerSettings.Address = "127.0.0.1:1";
    std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
        FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    Options.BrokerSettings.OffsetsForTimesTimeoutMS = 10;
    Options.BrokerSettings.MetadataTimeoutMS = 10;
    TestStreamer = std::make_unique<StreamerStandIn>(Options);
  }
  std::string ReaderKey{"test"};
  std::string DataBuffer{"0000test"};
  std::string SourceName{"SomeRandomSourceName"};
  KafkaW::BrokerSettings BrokerSettings;
  StreamerOptions Options;
  std::unique_ptr<StreamerStandIn> TestStreamer;
};

TEST_F(StreamerProcessTimingTest,
       pollAndProcessReturnsErrIfMessageHasNoTimestamp) {
  FlatbufferReaderRegistry::Registrar<StreamerNoTimestampTestDummyReader>
      RegisterIt(ReaderKey);
  TestStreamer->Options.StartTimestamp = std::chrono::milliseconds{1};
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<std::string, Source> SourceList;
  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
  SourceList.insert(std::move(TempPair));

  TestStreamer->setSources(SourceList);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(
          generateKafkaMsg(reinterpret_cast<const char *>(DataBuffer.c_str()),
                           DataBuffer.size()))
      .TIMES(1);
  TestStreamer->ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::ERR);
}

TEST_F(StreamerProcessTimingTest, MessageBeforeStartTimestamp) {
  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
      ReaderKey);
  TestStreamer->Options.StartTimestamp = std::chrono::milliseconds{1};
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<std::string, Source> SourceList;
  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
  SourceList.insert(std::move(TempPair));
  TestStreamer->setSources(SourceList);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(
          generateKafkaMsg(reinterpret_cast<const char *>(DataBuffer.c_str()),
                           DataBuffer.size()))
      .TIMES(1);
  TestStreamer->ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::OK);
}

class StreamerHighTimestampTestDummyReader
    : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override { return true; }
  std::string source_name(FlatbufferMessage const &Message) const override {
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t timestamp(FlatbufferMessage const &Message) const override {
    return 5000000;
  }
};

TEST_F(StreamerProcessTimingTest, MessageAfterStopTimestamp) {
  FlatbufferReaderRegistry::Registrar<StreamerHighTimestampTestDummyReader>
      RegisterIt(ReaderKey);
  TestStreamer->Options.StopTimestamp = std::chrono::milliseconds{1};
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<std::string, Source> SourceList;
  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
  SourceList.insert(std::move(TempPair));
  TestStreamer->setSources(SourceList);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(
          generateKafkaMsg(reinterpret_cast<const char *>(DataBuffer.c_str()),
                           DataBuffer.size()))
      .TIMES(1);
  TestStreamer->ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::STOP);
}

class DemuxerStandIn : public DemuxTopic {
public:
  explicit DemuxerStandIn(std::string Topic) : DemuxTopic(std::move(Topic)) {}
  MAKE_MOCK1(process_message, ProcessMessageResult(FlatbufferMessage const &),
             override);
};

TEST_F(StreamerProcessTimingTest, MessageTimeout) {
  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
      ReaderKey);
  TestStreamer->Options.StopTimestamp = std::chrono::milliseconds{1};
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<std::string, Source> SourceList;
  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
  SourceList.insert(std::move(TempPair));
  TestStreamer->setSources(SourceList);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  int CallCounter{0};
  auto PollResult = [this, &CallCounter]() {
    CallCounter++;
    if (CallCounter == 1) {
      return generateKafkaMsg(
          reinterpret_cast<const char *>(DataBuffer.c_str()),
          DataBuffer.size());
    }
    return generateEmptyKafkaMsg(PollStatus::EndOfPartition);
  };
  REQUIRE_CALL(*EmptyPollerConsumer, poll()).RETURN(PollResult()).TIMES(2);

  TestStreamer->ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxerStandIn Demuxer("SomeTopicName");
  REQUIRE_CALL(Demuxer, process_message(_))
      .RETURN(ProcessMessageResult::OK)
      .TIMES(1);
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::OK);
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::STOP);
}

TEST_F(StreamerProcessTimingTest, EmptyMessageAfterStop) {
  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
      ReaderKey);

  TestStreamer->Options.StopTimestamp = std::chrono::milliseconds{5};
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<std::string, Source> SourceList;
  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
  SourceList.insert(std::move(TempPair));
  TestStreamer->setSources(SourceList);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);

  TestStreamer->ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxerStandIn Demuxer("SomeTopicName");
  REQUIRE_CALL(Demuxer, process_message(_)).TIMES(0);
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::STOP);
}

TEST_F(StreamerProcessTimingTest, EmptyMessageBeforeStop) {
  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
      ReaderKey);
  auto Now = std::chrono::system_clock::now();
  auto Then = std::chrono::duration_cast<std::chrono::milliseconds>(
                  Now.time_since_epoch()) +
              std::chrono::milliseconds(12000);
  TestStreamer->Options.StopTimestamp = Then;
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<std::string, Source> SourceList;
  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
  SourceList.insert(std::move(TempPair));
  TestStreamer->setSources(SourceList);
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);

  TestStreamer->ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxerStandIn Demuxer("SomeTopicName");
  REQUIRE_CALL(Demuxer, process_message(_)).TIMES(0);
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::OK);
}

class StreamerMessageSlightlyAfterStopTestDummyReader
    : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override { return true; }
  std::string source_name(FlatbufferMessage const &Message) const override {
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t timestamp(FlatbufferMessage const &Message) const override {
    return 1;
  }
};

TEST_F(StreamerProcessTimingTest, EmptyMessageSlightlyAfterStop) {
  FlatbufferReaderRegistry::Registrar<
      StreamerMessageSlightlyAfterStopTestDummyReader>
      RegisterIt(ReaderKey);
  namespace c = std::chrono;
  auto Now = c::duration_cast<c::milliseconds>(
      c::system_clock::now().time_since_epoch());
  TestStreamer->Options.StopTimestamp = Now;
  TestStreamer->Options.AfterStopTime = c::milliseconds(20000);
  std::this_thread::sleep_for(c::milliseconds(5));
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<std::string, Source> SourceList;
  std::pair<std::string, Source> TempPair{SourceName, std::move(TestSource)};
  SourceList.insert(std::move(TempPair));
  TestStreamer->setSources(SourceList);

  BrokerSettings.OffsetsForTimesTimeoutMS = 10;
  BrokerSettings.MetadataTimeoutMS = 10;
  ConsumerEmptyStandIn *EmptyPollerConsumer =
      new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);

  TestStreamer->ConsumerCreated =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxerStandIn Demuxer("SomeTopicName");
  REQUIRE_CALL(Demuxer, process_message(_)).TIMES(0);
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::OK);
}
