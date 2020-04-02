// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <chrono>
#include <f142_logdata_generated.h>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>
#include <utility>

#include "DemuxTopic.h"
#include "Msg.h"
#include "Streamer.h"
#include "StreamerTestMocks.h"
#include "helpers/SetExtractorModule.h"

namespace FileWriter {

using Kafka::PollStatus;
using trompeloeil::_;
using namespace FileWriter;

std::pair<PollStatus, Msg> generateKafkaMsg(char const *DataPtr,
                                            size_t const Size,
                                            int64_t Offset = 0,
                                            int32_t Partition = 0) {
  auto MetaData = FileWriter::MessageMetaData{
      std::chrono::milliseconds(0),
      RdKafka::MessageTimestamp::MessageTimestampType::
          MSG_TIMESTAMP_CREATE_TIME,
      Offset, Partition};
  FileWriter::Msg Message = FileWriter::Msg(DataPtr, Size, MetaData);
  return {PollStatus::Message, std::move(Message)};
}

std::pair<PollStatus, Msg> generateEmptyKafkaMsg(PollStatus Status) {
  return {Status, FileWriter::Msg()};
}

std::pair<PollStatus, Msg> generateKafkaMsgWithValidFlatbuffer(
    std::string const &SourceName = "test_source", int32_t Value = 42,
    int64_t Offset = 0, int32_t Partition = 0) {

  flatbuffers::FlatBufferBuilder Builder;

  auto nameOffset = Builder.CreateString(SourceName);
  auto valueOffset = CreateInt(Builder, Value);
  uint64_t timestamp = 123456789;
  auto LogDataOffset = CreateLogData(Builder, nameOffset, Value::Int,
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

// Suppress false-positive from cppcheck
// cppcheck-suppress syntaxError
TEST_F(StreamerInitTest, CannotCreateStreamerWithoutProvidingABroker) {
  EXPECT_THROW(Streamer("", "topic", Options,
                        std::make_unique<Kafka::MockConsumer>(
                            StreamerOptions().BrokerSettings),
                        std::make_shared<DemuxerStandIn>("topic")),
               std::runtime_error);
}

TEST_F(StreamerInitTest, CannotCreateStreamerWithoutProvidingATopic) {
  EXPECT_THROW(Streamer("broker", "", Options,
                        std::make_unique<Kafka::MockConsumer>(
                            StreamerOptions().BrokerSettings),
                        std::make_shared<DemuxerStandIn>("topic")),
               std::runtime_error);
}

TEST_F(StreamerInitTest, CanCreateAStreamerIfProvideABrokerAndATopic) {
  EXPECT_NO_THROW(Streamer(
      "broker", "topic", Options,
      std::make_unique<Kafka::MockConsumer>(StreamerOptions().BrokerSettings),
      std::make_shared<DemuxerStandIn>("topic")));
}

class StreamerProcessTest : public ::testing::Test {
protected:
  void SetUp() override {
    BrokerSettings.Address = "127.0.0.1:1";
    Options.BrokerSettings = BrokerSettings;
  }
  Kafka::BrokerSettings BrokerSettings;
  StreamerOptions Options;
};

TEST_F(StreamerProcessTest, CreationNotYetDoneThenDoesNotPoll) {
  StreamerStandIn TestStreamer(Options);
  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll()).TIMES(0);
  TestStreamer.ConsumerInitialised.get();
  TestStreamer.ConsumerInitialised =
      std::async(std::launch::async, [EmptyPollerConsumer]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });
  TestStreamer.process();
}

TEST_F(StreamerProcessTest, InvalidFuture) {
  StreamerStandIn TestStreamer(Options);
  TestStreamer.ConsumerInitialised =
      std::future<std::pair<StreamerStatus, ConsumerPtr>>();
  EXPECT_THROW(TestStreamer.process(), std::runtime_error);
}

TEST_F(StreamerProcessTest,
       PollAndProcessReturnsErrorIfConsumerHasAConfigurationError) {
  StreamerStandIn TestStreamer(Options);
  TestStreamer.ConsumerInitialised = std::async(std::launch::async, []() {
    return std::pair<StreamerStatus, ConsumerPtr>{
        StreamerStatus::INITIALISATION_FAILED, nullptr};
  });
  EXPECT_THROW(TestStreamer.process(), std::runtime_error);
}

TEST_F(StreamerProcessTest,
       NumberOfProcessedMessagesDoesNotIncreaseOnEmptyMessage) {
  StreamerStandIn TestStreamer(Options);
  auto EmptyMessage = generateEmptyKafkaMsg(PollStatus::Empty);
  TestStreamer.processMessage(EmptyMessage);
  EXPECT_EQ(TestStreamer.getNumberProcessedMessages(), 0);
}

TEST_F(StreamerProcessTest,
       NumberOfProcessedMessagesDoesNotIncreaseIfEndOfPartitionIsReached) {
  StreamerStandIn TestStreamer(Options);
  auto TestMessage = generateEmptyKafkaMsg(PollStatus::EndOfPartition);
  TestStreamer.processMessage(TestMessage);
  EXPECT_EQ(TestStreamer.getNumberProcessedMessages(), 0);
}

TEST_F(StreamerProcessTest,
       NumberOfProcessedMessagesDoesNotIncreaseIfThereWasAnErrorPolling) {
  StreamerStandIn TestStreamer(Options);
  auto TestMessage = generateEmptyKafkaMsg(PollStatus::Error);
  TestStreamer.processMessage(TestMessage);
  EXPECT_EQ(TestStreamer.getNumberProcessedMessages(), 0);
}

TEST_F(
    StreamerProcessTest,
    NumberOfProcessedMessagesDoesNotIncreaseIfTryingToProcessInvalidMessage) {
  std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &Readers =
      FlatbufferReaderRegistry::getReaders();
  Readers.clear();
  char DataBuffer[]{"0000test"};
  std::string ReaderKey{"test"};

  auto TestMessage = generateKafkaMsg(DataBuffer, sizeof(DataBuffer));
  StreamerStandIn TestStreamer(Options);
  TestStreamer.processMessage(TestMessage);
  EXPECT_EQ(TestStreamer.getNumberProcessedMessages(), 0);
}

TEST_F(
    StreamerProcessTest,
    NumberOfProcessedMessagesDoesNotIncreaseIfMessageContainsUnknownSourceName) {

  std::string ReaderKey{"test"};
  setExtractorModule<StreamerTestDummyReader>(ReaderKey);

  char DataBuffer[]{"0000test"};

  auto TestMessage = generateKafkaMsg(static_cast<const char *>(DataBuffer),
                                      sizeof(DataBuffer));
  StreamerStandIn TestStreamer(Options);
  TestStreamer.processMessage(TestMessage);
  EXPECT_EQ(TestStreamer.getNumberProcessedMessages(), 0);
}

class StreamerProcessTimingTest : public ::testing::Test {
protected:
  void SetUp() override {
    setExtractorModule<StreamerHighTimestampTestDummyReader>(SchemaID);
    BrokerSettings.Address = "127.0.0.1:1";
    Options.BrokerSettings.OffsetsForTimesTimeoutMS = 10;
    Options.BrokerSettings.MetadataTimeoutMS = 10;
    WriterModule::ptr Writer(new WriterModuleStandIn());
    FileWriter::Source TestSource(SourceName, SchemaID, TopicName,
                                  std::move(Writer));
    DemuxPtr Demuxer = std::make_shared<DemuxerStandIn>(SourceName);
    Demuxer->addSource(std::move(TestSource));
    TestStreamer = std::make_unique<StreamerStandIn>(Options, Demuxer);
  }
  std::string SchemaID{"f142"};
  std::string DataBuffer{"0000test"};
  std::string SourceName{"SomeRandomSourceName"};
  std::string TopicName{"SomeRandomTopicName"};
  Kafka::BrokerSettings BrokerSettings;
  StreamerOptions Options;
  std::unique_ptr<StreamerStandIn> TestStreamer;
};

TEST_F(StreamerProcessTimingTest,
       NumberOfProcessedMessagesDoesNotIncreaseIfMessageHasNoTimestamp) {
  auto TestMessage = generateKafkaMsg(
      reinterpret_cast<const char *>(DataBuffer.c_str()), DataBuffer.size());

  TestStreamer->processMessage(TestMessage);
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 0);
}

TEST_F(StreamerProcessTimingTest,
       NumberOfProcessedMessagesDoesNotIncreaseIfMessageBeforeStartTimestamp) {
  TestStreamer->setStartTime(std::chrono::milliseconds{1});
  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(
          generateKafkaMsg(reinterpret_cast<const char *>(DataBuffer.c_str()),
                           DataBuffer.size()))
      .TIMES(1);
  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });

  TestStreamer->process();
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 0);
}

TEST_F(StreamerProcessTimingTest,
       NumberOfProcessedMessagesDoesNotIncreaseIfOffsetIsReached) {
  TestStreamer->setStopTime(std::chrono::milliseconds{1});
  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);

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
  // Start offsets are queried first, this is in order to check that there are
  // actually
  // messages between the start and stop times which we should wait to consume
  // We'll pretend we started at offset 0
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{0})
      .TIMES(1);

  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });
  TestStreamer->process();
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 0);
}

TEST_F(
    StreamerProcessTimingTest,
    NumberOfProcessedMessagesDoesNotIncreaseWhenStopOffsetHasAlreadyBeenReached) {
  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);

  // The newly received message will have an offset of 10
  int64_t StopOffset = 10;

  // Second message which will be received
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);
  // First message which will be received
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateKafkaMsgWithValidFlatbuffer(SourceName, 42, StopOffset))
      .TIMES(1);
  // Start offsets are queried first, this is in order to check that there are
  // actually
  // messages between the start and stop times which we should wait to consume
  // We'll pretend we started at offset 0
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{0})
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
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });

  // A message will be received here
  // however no stop time has been set yet, so expect increase
  TestStreamer->process();
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 1);

  TestStreamer->setStopTime(std::chrono::milliseconds{1});

  // We already reached the stop offset, so the number of processed messages
  // doesn't increase
  TestStreamer->process();
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 1);
}

TEST_F(
    StreamerProcessTimingTest,
    NumberOfProcessedMessagesDoesNotIncreaseWhenThereIsNoDataOnTopicToConsume) {
  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);

  // There is no data, so polling the consumer will give us an EndOfPartition
  // message with no payload
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);

  // No data on topic so stop offset will be reported by RdKafka as -1
  int64_t StopOffset = -1;
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{StopOffset})
      .TIMES(1);
  // Start offsets are queried first, this is in order to check that there are
  // actually
  // messages between the start and stop times which we should wait to consume
  // There is no data on the topic, so both start and stop offset will be
  // reported as -1
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{-1})
      .TIMES(1);
  REQUIRE_CALL(*EmptyPollerConsumer, getHighWatermarkOffset(_, _)).RETURN(-1);
  ALLOW_CALL(*EmptyPollerConsumer, getCurrentOffsets(_))
      .RETURN(std::vector<int64_t>{0});
  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });

  TestStreamer->setStopTime(std::chrono::milliseconds{1});

  TestStreamer->process();
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 0);
}

TEST_F(
    StreamerProcessTimingTest,
    NumberOfProcessedMessagesDoesNotIncreaseWhenThereIsNoDataBetweenRunStartAndStop) {
  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);

  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);

  // No data between start and stop time so the start and stop offsets are the
  // same
  int64_t StopOffset = 42;
  int64_t StartOffset = StopOffset;
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{StopOffset})
      .TIMES(1);
  // Start offsets are queried first, this is in order to check that there are
  // actually
  // messages between the start and stop times which we should wait to consume
  // There is no data on the topic, so both start and stop offset will be
  // reported as -1
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{StartOffset})
      .TIMES(1);
  ALLOW_CALL(*EmptyPollerConsumer, getCurrentOffsets(_))
      .RETURN(std::vector<int64_t>{0});
  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });

  TestStreamer->setStopTime(std::chrono::milliseconds{1});

  TestStreamer->process();
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 0);
}

TEST_F(StreamerProcessTimingTest,
       ReceivingEmptyMessageAfterStopIsNotProcessed) {
  TestStreamer->setStopTime(std::chrono::milliseconds{5});

  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);
  int64_t StopOffset = 2;
  ALLOW_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{StopOffset});
  // Start offsets are queried first, this is in order to check that there are
  // actually
  // messages between the start and stop times which we should wait to consume
  // We'll pretend we started at offset 0
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{0})
      .TIMES(1);
  ALLOW_CALL(*EmptyPollerConsumer, getCurrentOffsets(_))
      .RETURN(std::vector<int64_t>{0});

  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });

  TestStreamer->process();
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 0);
}

TEST_F(StreamerProcessTimingTest, EmptyMessageBeforeStopAreNotProcessed) {
  auto Now = std::chrono::system_clock::now();
  auto Then = std::chrono::duration_cast<std::chrono::milliseconds>(
                  Now.time_since_epoch()) +
              std::chrono::milliseconds(12000);
  TestStreamer->setStopTime(Then);

  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);

  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });
  TestStreamer->process();
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 0);
}

TEST_F(StreamerProcessTimingTest,
       EmptyMessageSlightlyAfterStopAreNotProcessed) {
  auto Now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  TestStreamer->setStopTime(Now);
  std::this_thread::sleep_for(std::chrono::milliseconds(5));

  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateEmptyKafkaMsg(PollStatus::EndOfPartition))
      .TIMES(1);

  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });

  TestStreamer->process();
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 0);
}

TEST_F(StreamerProcessTimingTest, MessageAfterStopTimeIsOkButNotProcessed) {
  TestStreamer->setStartTime(std::chrono::milliseconds{1});
  // Message timestamp returned is higher than this
  TestStreamer->setStopTime(std::chrono::milliseconds{2});
  WriterModule::ptr Writer(new WriterModuleStandIn());

  FileWriter::Source TestSource(SourceName, SchemaID, TopicName,
                                std::move(Writer));
  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);
  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateKafkaMsgWithValidFlatbuffer(SourceName))
      .TIMES(1);
  int64_t StopOffset = 10;
  ALLOW_CALL(*EmptyPollerConsumer, getCurrentOffsets(_))
      .RETURN(std::vector<int64_t>{StopOffset - 2});
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{StopOffset})
      .TIMES(1);
  REQUIRE_CALL(*EmptyPollerConsumer, offsetsForTimesAllPartitions(_, _))
      .RETURN(std::vector<int64_t>{0})
      .TIMES(1);
  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });

  TestStreamer->process();
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 0);
}

TEST(FlatBufferValidationTest,
     NumberOfValidationFailuresIncreasesIfFlatBufferIsInvalid) {
  Kafka::BrokerSettings BrokerSettings;
  StreamerOptions Options;
  std::unique_ptr<StreamerStandIn> TestStreamer;
  std::string SchemaID = "f142";
  std::string SourceName{"SomeRandomSourceName"};
  std::string TopicName{"SomeRandomTopicName"};
  setExtractorModule<StreamerMessageFailsValidationTestDummyReader>(SchemaID);
  WriterModule::ptr Writer(new WriterModuleStandIn());
  FileWriter::Source TestSource(SourceName, SchemaID, TopicName,
                                std::move(Writer));
  DemuxPtr Demuxer = std::make_shared<DemuxerStandIn>(SourceName);
  Demuxer->addSource(std::move(TestSource));
  TestStreamer = std::make_unique<StreamerStandIn>(Options, Demuxer);

  auto *EmptyPollerConsumer = new Kafka::MockConsumer(BrokerSettings);

  REQUIRE_CALL(*EmptyPollerConsumer, poll())
      .RETURN(generateKafkaMsgWithValidFlatbuffer(SourceName, 42, 10))
      .TIMES(1);

  TestStreamer->ConsumerInitialised =
      std::async(std::launch::async, [&EmptyPollerConsumer]() {
        return std::pair<StreamerStatus, ConsumerPtr>{StreamerStatus::OK,
                                                      EmptyPollerConsumer};
      });
  TestStreamer->process();

  EXPECT_EQ(TestStreamer->getNumberFailedValidation(), 1);
  EXPECT_EQ(TestStreamer->getNumberProcessedMessages(), 0);
}
} // namespace FileWriter
