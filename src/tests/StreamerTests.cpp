#include <Msg.h>
#include <chrono>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>
#include <utility>

#include "Streamer.h"
#include "StreamerTestMocks.h"
#include "schemas/f142/FlatbufferReader.h"

namespace FileWriter {

using KafkaW::PollStatus;
using trompeloeil::_;
using namespace FileWriter;

std::unique_ptr<std::pair<PollStatus, Msg>>
generateKafkaMsg(char const *DataPtr, size_t const Size, int64_t Offset = 0,
                 int32_t Partition = 0) {
  FileWriter::Msg Message = FileWriter::Msg::owned(DataPtr, Size);
  Message.MetaData = FileWriter::MessageMetaData{
      std::chrono::milliseconds(0),
      RdKafka::MessageTimestamp::MessageTimestampType::
          MSG_TIMESTAMP_CREATE_TIME,
      Offset, Partition};
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

std::unique_ptr<std::pair<PollStatus, Msg>> generateKafkaMsgWithValidFlatbuffer(
    std::string const &SourceName = "test_source", int32_t Value = 42,
    int64_t Offset = 0, int32_t Partition = 0) {

  flatbuffers::FlatBufferBuilder Builder;

  auto nameOffset = Builder.CreateString(SourceName);
  auto valueOffset = Schemas::f142::CreateInt(Builder, Value);
  uint64_t timestamp = 1234;
  auto LogDataOffset =
      CreateLogData(Builder, nameOffset, Schemas::f142::Value::Int,
                    valueOffset.Union(), timestamp);

  FinishLogDataBuffer(Builder, LogDataOffset);
  flatbuffers::DetachedBuffer MessageBuffer = Builder.Release();

  return generateKafkaMsg(reinterpret_cast<const char *>(MessageBuffer.data()),
                          MessageBuffer.size(), Offset, Partition);
}

class StreamerInitTest : public ::testing::Test {
protected:
  void SetUp() override { Options.BrokerSettings.MetadataTimeoutMS = 10; }
  StreamerOptions Options;
};

TEST_F(StreamerInitTest, CannotCreateStreamerWithoutProvidingABroker) {
  EXPECT_THROW(
      Streamer("", "topic", Options, std::make_unique<ConsumerEmptyStandIn>(
                                         StreamerOptions().BrokerSettings)),
      std::runtime_error);
}

TEST_F(StreamerInitTest, CannotCreateStreamerWithoutProvidingATopic) {
  EXPECT_THROW(
      Streamer("broker", "", Options, std::make_unique<ConsumerEmptyStandIn>(
                                          StreamerOptions().BrokerSettings)),
      std::runtime_error);
}

TEST_F(StreamerInitTest, CanCreateAStreamerIfProvideABrokerAndATopic) {
  EXPECT_NO_THROW(Streamer("broker", "topic", Options,
                           std::make_unique<ConsumerEmptyStandIn>(
                               StreamerOptions().BrokerSettings)));
}

class StreamerProcessTest : public ::testing::Test {
protected:
  void SetUp() override {
    BrokerSettings.Address = "127.0.0.1:1";
    Options.BrokerSettings = BrokerSettings;
  }
  KafkaW::BrokerSettings BrokerSettings;
  StreamerOptions Options;
};

TEST_F(StreamerProcessTest, CreationNotYetDone) {
  StreamerStandIn TestStreamer(Options);
  auto *EmptyPollerConsumer = new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll()).TIMES(0);
  TestStreamer.ConsumerInitialised.get();
  TestStreamer.ConsumerInitialised =
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
  TestStreamer.ConsumerInitialised =
      std::future<std::pair<Status::StreamerStatus, ConsumerPtr>>();
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_THROW(TestStreamer.pollAndProcess(Demuxer), std::runtime_error);
}

TEST_F(StreamerProcessTest,
       PollAndProcessReturnsErrorIfConsumerHasAConfigurationError) {
  StreamerStandIn TestStreamer(Options);
  TestStreamer.ConsumerInitialised = std::async(std::launch::async, []() {
    return std::pair<Status::StreamerStatus, ConsumerPtr>{
        Status::StreamerStatus::CONFIGURATION_ERROR, nullptr};
  });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_THROW(TestStreamer.pollAndProcess(Demuxer), std::runtime_error);
}

TEST_F(StreamerProcessTest,
       ProcessMessageReturnsOkResultIfMessageWithNoPayloadIsReceived) {
  StreamerStandIn TestStreamer(Options);
  DemuxTopic Demuxer("SomeTopicName");
  auto EmptyMessage = generateEmptyKafkaMsg(PollStatus::Empty);
  EXPECT_EQ(TestStreamer.processMessage(Demuxer, EmptyMessage),
            ProcessMessageResult::OK);
}

TEST_F(StreamerProcessTest,
       ProcessMessageReturnsOkResultIfEndOfPartitionIsReached) {
  StreamerStandIn TestStreamer(Options);
  DemuxTopic Demuxer("SomeTopicName");
  auto TestMessage = generateEmptyKafkaMsg(PollStatus::EndOfPartition);
  EXPECT_EQ(TestStreamer.processMessage(Demuxer, TestMessage),
            ProcessMessageResult::OK);
}

TEST_F(StreamerProcessTest,
       ProcessMessageReturnsErrorResultIfThereWasAnErrorPolling) {
  StreamerStandIn TestStreamer(Options);
  DemuxTopic Demuxer("SomeTopicName");
  auto TestMessage = generateEmptyKafkaMsg(PollStatus::Error);
  EXPECT_EQ(TestStreamer.processMessage(Demuxer, TestMessage),
            ProcessMessageResult::ERR);
}

TEST_F(StreamerProcessTest, TryingToProcessInvalidMessageReturnsError) {
  std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
      FlatbufferReaderRegistry::getReaders();
  Readers.clear();
  char DataBuffer[]{"0000test"};
  std::string ReaderKey{"test"};

  auto MessageNotContainingValidFlatbuffer =
      generateKafkaMsg(DataBuffer, sizeof(DataBuffer));
  StreamerStandIn TestStreamer(Options);
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(
      TestStreamer.processMessage(Demuxer, MessageNotContainingValidFlatbuffer),
      ProcessMessageResult::ERR);
}

TEST_F(StreamerProcessTest,
       ProcessMessageReturnsOkIfMessageContainsUnknownSourceName) {
  std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
      FlatbufferReaderRegistry::getReaders();
  Readers.clear();
  std::string ReaderKey{"test"};

  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
      ReaderKey);
  char DataBuffer[]{"0000test"};

  auto MessageWithUnknownSourceName = generateKafkaMsg(
      static_cast<const char *>(DataBuffer), sizeof(DataBuffer));
  StreamerStandIn TestStreamer(Options);
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer.processMessage(Demuxer, MessageWithUnknownSourceName),
            ProcessMessageResult::OK);
}

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
       ProcessMessageReturnsErrIfMessageHasNoTimestamp) {
  FlatbufferReaderRegistry::Registrar<StreamerNoTimestampTestDummyReader>
      RegisterIt(ReaderKey);
  TestStreamer->Options.StartTimestamp = std::chrono::milliseconds{1};
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<FlatbufferMessage::SrcHash, Source> SourceList;
  std::pair<FlatbufferMessage::SrcHash, Source> TempPair{TestSource.getHash(),
                                                         std::move(TestSource)};
  SourceList.insert(std::move(TempPair));

  auto MessageWithNoTimestamp = generateKafkaMsg(
      reinterpret_cast<const char *>(DataBuffer.c_str()), DataBuffer.size());

  TestStreamer->setSources(SourceList);
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer->processMessage(Demuxer, MessageWithNoTimestamp),
            ProcessMessageResult::ERR);
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
  std::unordered_map<FlatbufferMessage::SrcHash, Source> SourceList;
  std::pair<FlatbufferMessage::SrcHash, Source> TempPair{TestSource.getHash(),
                                                         std::move(TestSource)};
  SourceList.insert(std::move(TempPair));
  TestStreamer->setSources(SourceList);
  auto *EmptyPollerConsumer = new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(
          generateKafkaMsg(reinterpret_cast<const char *>(DataBuffer.c_str()),
                           DataBuffer.size()))
      .TIMES(1);
  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::OK);
}

TEST_F(StreamerProcessTimingTest,
       ProcessMessageReturnsStopWhenStopOffsetIsReached) {
  FlatbufferReaderRegistry::Registrar<StreamerHighTimestampTestDummyReader>
      RegisterIt("f142");
  TestStreamer->Options.StopTimestamp = std::chrono::milliseconds{1};
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<FlatbufferMessage::SrcHash, Source> SourceList;
  SourceList.emplace(TestSource.getHash(), std::move(TestSource));
  TestStreamer->setSources(SourceList);
  auto *EmptyPollerConsumer = new ConsumerEmptyStandIn(BrokerSettings);

  // The newly received message will have an offset of 10
  int64_t StopOffset = 10;

  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateKafkaMsgWithValidFlatbuffer(SourceName, 42, StopOffset))
      .TIMES(1);
  // When current offsets are looked up, report that we are on the offset before
  // the stop offset
  ALLOW_CALL(*EmptyPollerConsumer, getCurrentOffsets(_))
      .RETURN(std::vector<int64_t>{StopOffset - 1});

  // The consumer will report that there is one partition and we should stop at
  // offset 10
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{StopOffset})
      .TIMES(1);
  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");

  // We've reached the stop offset, so STOP should be returned
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::STOP);
}

TEST_F(StreamerProcessTimingTest,
       ProcessMessageReturnsStopWhenStopOffsetHasAlreadyBeenReached) {
  FlatbufferReaderRegistry::Registrar<StreamerHighTimestampTestDummyReader>
      RegisterIt("f142");
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  std::string const HistoricalDataSourceName = "fw-test-helpers";
  FileWriter::Source TestSource(HistoricalDataSourceName, ReaderKey,
                                std::move(Writer));
  std::unordered_map<FlatbufferMessage::SrcHash, Source> SourceList;
  SourceList.emplace(TestSource.getHash(), std::move(TestSource));
  TestStreamer->setSources(SourceList);
  auto *EmptyPollerConsumer = new ConsumerEmptyStandIn(BrokerSettings);

  // The newly received message will have an offset of 10
  int64_t StopOffset = 10;

  // Second message which will be received
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);
  // First message which will be received
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateKafkaMsgWithValidFlatbuffer(HistoricalDataSourceName, 42,
                                                  StopOffset))
      .TIMES(1);
  // The consumer will report that there is one partition and we should stop at
  // offset 10
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{StopOffset})
      .TIMES(1);
  REQUIRE_CALL(*EmptyPollerConsumer, getCurrentOffsets(_))
      .RETURN(std::vector<int64_t>{StopOffset})
      .TIMES(1);
  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxTopic Demuxer("SomeTopicName");

  // A message will be received here with an offset of 10
  // however no stop time has been set yet, so expect OK
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::OK);

  TestStreamer->Options.StopTimestamp = std::chrono::milliseconds{1};

  // We already reached the stop offset, so STOP should be returned
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::STOP);
}

TEST_F(StreamerProcessTimingTest, ReceivingEmptyMessageAfterStopIsOk) {
  // ProcessMessage will return Ok, because the message timestamp is after
  // the stop time so the empty payload is not accessed

  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
      ReaderKey);

  TestStreamer->Options.StopTimestamp = std::chrono::milliseconds{5};
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<FlatbufferMessage::SrcHash, Source> SourceList;
  SourceList.emplace(TestSource.getHash(), std::move(TestSource));
  TestStreamer->setSources(SourceList);
  auto *EmptyPollerConsumer = new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);
  std::vector<int64_t> ReturnedOffsets = {1};
  ALLOW_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(ReturnedOffsets);
  ALLOW_CALL(*EmptyPollerConsumer, getCurrentOffsets(_))
      .RETURN(std::vector<int64_t>{0});

  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxerStandIn Demuxer("SomeTopicName");
  REQUIRE_CALL(Demuxer, process_message(_)).TIMES(0);
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::OK);
}

TEST_F(StreamerProcessTimingTest, NoMessagesInTopicAfterStopTime) {
  FlatbufferReaderRegistry::Registrar<StreamerTestDummyReader> RegisterIt(
      ReaderKey);

  TestStreamer->Options.StopTimestamp = std::chrono::milliseconds{5};
  HDFWriterModule::ptr Writer(new WriterModuleStandIn());
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), flush())
      .RETURN(0);
  ALLOW_CALL(*dynamic_cast<WriterModuleStandIn *>(Writer.get()), close())
      .RETURN(0);
  FileWriter::Source TestSource(SourceName, ReaderKey, std::move(Writer));
  std::unordered_map<FlatbufferMessage::SrcHash, Source> SourceList;
  std::pair<FlatbufferMessage::SrcHash, Source> TempPair{TestSource.getHash(),
                                                         std::move(TestSource)};
  SourceList.insert(std::move(TempPair));
  TestStreamer->setSources(SourceList);
  auto *EmptyPollerConsumer = new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);

  // GIVEN that there are no messages in the topic, and therefore
  // offsetsForTimes returns -1 as the stop offset
  std::vector<int64_t> ReturnedOffsets = {-1};
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(ReturnedOffsets);
  // and high watermark offset is also -1
  REQUIRE_CALL(*EmptyPollerConsumer, getHighWatermarkOffset(_, _)).RETURN(-1);
  ALLOW_CALL(*EmptyPollerConsumer, getCurrentOffsets(_))
      .RETURN(std::vector<int64_t>{0});

  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxerStandIn Demuxer("SomeTopicName");
  REQUIRE_CALL(Demuxer, process_message(_)).TIMES(0);
  // WHEN pollAndProcess is run
  // THEN expect STOP response, as there is no need to continue trying to
  // consume data from this topic
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
  std::unordered_map<FlatbufferMessage::SrcHash, Source> SourceList;
  std::pair<FlatbufferMessage::SrcHash, Source> TempPair{TestSource.getHash(),
                                                         std::move(TestSource)};
  SourceList.insert(std::move(TempPair));
  TestStreamer->setSources(SourceList);
  auto *EmptyPollerConsumer = new ConsumerEmptyStandIn(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);

  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<Status::StreamerStatus, ConsumerPtr>{
            Status::StreamerStatus::OK, EmptyPollerConsumer};
      });
  DemuxerStandIn Demuxer("SomeTopicName");
  REQUIRE_CALL(Demuxer, process_message(_)).TIMES(0);
  EXPECT_EQ(TestStreamer->pollAndProcess(Demuxer), ProcessMessageResult::OK);
}

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
  std::unordered_map<FlatbufferMessage::SrcHash, Source> SourceList;
  std::pair<FlatbufferMessage::SrcHash, Source> TempPair{TestSource.getHash(),
                                                         std::move(TestSource)};
  SourceList.insert(std::move(TempPair));
  TestStreamer->setSources(SourceList);

  auto EmptyMessage = generateEmptyKafkaMsg(PollStatus::EndOfPartition);
  DemuxerStandIn Demuxer("SomeTopicName");
  EXPECT_EQ(TestStreamer->processMessage(Demuxer, EmptyMessage),
            ProcessMessageResult::OK);
}
} // namespace
