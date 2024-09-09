// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatbufferReader.h"
#include "Metrics/Registrar.h"
#include "Stream/MessageWriter.h"
#include "Stream/Partition.h"
#include "TimeUtility.h"
#include "WriterModuleBase.h"
#include "helpers/KafkaMocks.h"
#include "helpers/RdKafkaMocks.h"
#include "helpers/SetExtractorModule.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>

using std::chrono_literals::operator""s;
using trompeloeil::_;

class SourceFilterStandInAlt : public Stream::SourceFilter {
public:
  SourceFilterStandInAlt()
      : SourceFilter(std::chrono::system_clock::now(),
                     std::chrono::system_clock::now(), true, nullptr,
                     std::make_unique<Metrics::Registrar>("some_prefix")) {}
  MAKE_MOCK1(filter_message, bool(FileWriter::FlatbufferMessage const &Message),
             override);
  MAKE_CONST_MOCK0(has_finished, bool(), override);
};

class zzzzFbReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &) const override {
    return true;
  }
  std::string
  source_name(FileWriter::FlatbufferMessage const &) const override {
    return zzzzFbReader::UsedSourceName;
  }
  uint64_t timestamp(FileWriter::FlatbufferMessage const &) const override {
    return 1;
  }
  static std::string UsedSourceName;
};
std::string zzzzFbReader::UsedSourceName{"some_name"};

class PartitionStandIn : public Stream::Partition {
public:
  PartitionStandIn(std::unique_ptr<Kafka::ConsumerInterface> Consumer,
                   int Partition, std::string TopicName,
                   Stream::SrcToDst const &Map, Stream::MessageWriter *Writer,
                   time_point Start, time_point Stop, duration StopLeeway,
                   duration KafkaErrorTimeout,
                   std::function<bool()> AreStreamersPausedFunction)
      : Stream::Partition(
            std::move(Consumer), Partition, std::move(TopicName), Map, Writer,
            std::make_unique<Metrics::Registrar>("some_prefix").get(), Start,
            Stop, StopLeeway, KafkaErrorTimeout, AreStreamersPausedFunction) {}
  void addPollTask() override {
    // Do nothing as don't want to automatically poll again
  }
  using Partition::_consumer;
  using Partition::_executor;
  using Partition::_source_filters;
  using Partition::_stop_time;
  using Partition::_stop_time_leeway;
  using Partition::FlatbufferErrors;
  using Partition::forceStop;
  using Partition::KafkaErrors;
  using Partition::KafkaTimeouts;
  using Partition::MessagesProcessed;
  using Partition::MessagesReceived;
  using Partition::pollForMessage;
  using Partition::processMessage;
  MAKE_CONST_MOCK1(sleep, void(const duration Duration), override);
};

void waitUntilDoneProcessing(PartitionStandIn *UnderTest) {
  // Queue a job in the executor and block until it is complete
  // so that we know previously queued job that is part of test should
  // now have been executed
  std::promise<bool> Promise;
  auto Future = Promise.get_future();
  UnderTest->_executor.sendWork([&Promise]() { Promise.set_value(true); });
  Future.wait();
}

class MessageWriterStandIn : public Stream::MessageWriter {
public:
  MessageWriterStandIn()
      : Stream::MessageWriter(
            []() {}, 1s, std::make_unique<Metrics::Registrar>("some_prefix")) {}
  void addMessage(Stream::Message const &) override {}

protected:
  void writeMsgImpl(WriterModule::Base *,
                    FileWriter::FlatbufferMessage const &) override {}
};

class PartitionTest : public ::testing::Test {
public:
  auto createTestedInstance(
      time_point StopTime = time_point::max(),
      std::function<bool()> AreStreamersPausedFunction = []() {
        return false;
      }) {
    Kafka::BrokerSettings BrokerSettingsForTest;
    auto Temp = std::make_unique<PartitionStandIn>(
        std::make_unique<Kafka::MockConsumer>(BrokerSettingsForTest),
        UsedPartitionId, TopicName, UsedMap, nullptr, Start, StopTime,
        StopLeeway, ErrorTimeout, AreStreamersPausedFunction);
    Stop = StopTime;
    Consumer = dynamic_cast<Kafka::MockConsumer *>(Temp->_consumer.get());
    return Temp;
  }
  auto createTestedInstance(std::function<bool()> AreStreamersPausedFunction) {
    return createTestedInstance(time_point::max(), AreStreamersPausedFunction);
  }

  Kafka::MockConsumer *Consumer{nullptr};
  int UsedPartitionId{0};
  std::string TopicName{"some_topic"};
  size_t UsedFilterHash{
      FileWriter::calcSourceHash("zzzz", zzzzFbReader::UsedSourceName)};

  Stream::SrcToDst UsedMap{Stream::SrcDstKey{UsedFilterHash, UsedFilterHash,
                                             nullptr, "some_name", "idid",
                                             "idid_alt", true}};
  time_point Start{std::chrono::system_clock::now()};
  time_point Stop{std::chrono::system_clock::time_point::max()};
  duration StopLeeway{5s};
  duration ErrorTimeout{10s};
  std::array<char, 9> SomeData{'z', 'z', 'z', 'z', 'z', 'z', 'z', 'z', 'z'};
};

TEST_F(PartitionTest, OnConstructionValuesAreAsExpected) {
  auto StopTime = Start + 20s;
  auto UnderTest = createTestedInstance(StopTime);
  EXPECT_EQ(UnderTest->getPartitionID(), UsedPartitionId);
  EXPECT_EQ(UnderTest->getTopicName(), TopicName);
  EXPECT_EQ(UnderTest->_stop_time_leeway, StopLeeway);
  EXPECT_EQ(UnderTest->_stop_time, StopTime);
}

TEST_F(PartitionTest, IfStopTimeTooCloseToMaxThenItIsBackedOff) {
  auto StopTime = std::chrono::system_clock::time_point::max() - StopLeeway / 2;
  auto UnderTest = createTestedInstance(StopTime);
  EXPECT_EQ(UnderTest->_stop_time, StopTime - StopLeeway);
}

TEST_F(PartitionTest, ActualMessageIsCounted) {
  Kafka::MockConsumer::PollReturnType PollReturn;
  PollReturn.first = Kafka::PollStatus::Message;
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  UnderTest->pollForMessage();
  EXPECT_EQ(int(UnderTest->MessagesReceived), 1);
}

TEST_F(PartitionTest, DoesNotPollIfPaused) {
  auto IsPausedLambda = []() { return true; };
  auto UnderTest = createTestedInstance(IsPausedLambda);
  FORBID_CALL(*Consumer, poll());
  REQUIRE_CALL(*UnderTest, sleep(_)).TIMES(1);
  UnderTest->pollForMessage();
  EXPECT_EQ(int(UnderTest->MessagesReceived), 0);
}

TEST_F(PartitionTest, PollsIfResumedAfterPause) {
  Kafka::MockConsumer::PollReturnType PollReturn;
  PollReturn.first = Kafka::PollStatus::Message;
  bool IsPaused = false;
  auto IsPausedLambda = [&IsPaused]() { return IsPaused; };
  auto UnderTest = createTestedInstance(IsPausedLambda);
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  REQUIRE_CALL(*UnderTest, sleep(_)).TIMES(1);
  IsPaused = true;
  UnderTest->pollForMessage();
  IsPaused = false;
  UnderTest->pollForMessage();
  EXPECT_EQ(int(UnderTest->MessagesReceived), 1);
}

TEST_F(PartitionTest, TimeoutMessageIsCountedButThenIgnored) {
  Kafka::MockConsumer::PollReturnType PollReturn;
  PollReturn.first = Kafka::PollStatus::TimedOut;
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  UnderTest->pollForMessage();
  EXPECT_EQ(int(UnderTest->MessagesReceived), 0);
  EXPECT_EQ(int(UnderTest->KafkaTimeouts), 1);
}

TEST_F(PartitionTest, ErrorMessageIsCountedButThenIgnored) {
  Kafka::MockConsumer::PollReturnType PollReturn;
  PollReturn.first = Kafka::PollStatus::Error;
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  UnderTest->pollForMessage();
  EXPECT_EQ(int(UnderTest->MessagesReceived), 0);
  EXPECT_EQ(int(UnderTest->KafkaErrors), 1);
}

TEST_F(PartitionTest, EndOfPartitionMessageIsIgnored) {
  Kafka::MockConsumer::PollReturnType PollReturn;
  PollReturn.first = Kafka::PollStatus::EndOfPartition;
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  UnderTest->pollForMessage();
  EXPECT_EQ(int(UnderTest->MessagesReceived), 0);
}

TEST_F(PartitionTest, WithNoFiltersPartitionIsFinishedOnMessage) {
  Kafka::MockConsumer::PollReturnType PollReturn;
  PollReturn.first = Kafka::PollStatus::Message;
  auto UnderTest = createTestedInstance();
  UnderTest->_source_filters.clear();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  UnderTest->pollForMessage();
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, MessageWithInvalidFlatBufferIsNotProcessed) {
  FileWriter::MessageMetaData MetaData{
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (Start + 10s).time_since_epoch()),
      RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 0, 0,
      "::some_topic::"};
  uint8_t *TempPointer{nullptr};
  Kafka::MockConsumer::PollReturnType PollReturn{
      Kafka::PollStatus::Message, FileWriter::Msg{TempPointer, 0, MetaData}};
  auto UnderTest = createTestedInstance();
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  UnderTest->pollForMessage();
  EXPECT_EQ(int(UnderTest->MessagesReceived), 1);
  EXPECT_EQ(int(UnderTest->FlatbufferErrors), 1);
}

TEST_F(PartitionTest, MessageWithinStopLeewayDoesNotTriggerFinished) {
  Stop = Start + 20s;
  FileWriter::MessageMetaData MetaData{
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (Stop + StopLeeway).time_since_epoch()),
      RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 0, 0,
      "::some_topic::"};
  uint8_t *TempPointer{nullptr};
  Kafka::MockConsumer::PollReturnType PollReturn{
      Kafka::PollStatus::Message, FileWriter::Msg{TempPointer, 0, MetaData}};
  auto UnderTest = createTestedInstance(Stop);
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  UnderTest->pollForMessage();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, MessageAfterStopLeewayTriggersFinished) {
  Stop = Start + 20s;
  FileWriter::MessageMetaData MetaData{
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (Stop + StopLeeway + 1s).time_since_epoch()),
      RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 0, 0,
      "::some_topic::"};
  uint8_t *TempPointer{nullptr};
  Kafka::MockConsumer::PollReturnType PollReturn{
      Kafka::PollStatus::Message, FileWriter::Msg{TempPointer, 0, MetaData}};
  auto UnderTest = createTestedInstance(Stop);
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  UnderTest->pollForMessage();
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, ForceStopStops) {
  FileWriter::MessageMetaData MetaData{
      std::chrono::duration_cast<std::chrono::milliseconds>(
          Start.time_since_epoch()),
      RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 0, 0,
      "::some_topic::"};
  uint8_t *TempPointer{nullptr};
  Kafka::MockConsumer::PollReturnType PollReturn{
      Kafka::PollStatus::Message, FileWriter::Msg{TempPointer, 0, MetaData}};
  auto UnderTest = createTestedInstance(Stop);
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));
  UnderTest->pollForMessage();
  EXPECT_FALSE(UnderTest->hasFinished());
  UnderTest->forceStop();
  UnderTest->pollForMessage();
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, ForceStopWhenPausedStops) {
  bool IsPaused = true;
  auto IsPausedLambda = [&IsPaused]() { return IsPaused; };
  auto UnderTest = createTestedInstance(Stop, IsPausedLambda);
  FORBID_CALL(*Consumer, poll());
  ALLOW_CALL(*UnderTest, sleep(_));
  UnderTest->pollForMessage();
  EXPECT_FALSE(UnderTest->hasFinished());
  UnderTest->forceStop();
  UnderTest->pollForMessage();
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, FiltersAreInitialisedWithOriginalStoptime) {
  auto StopTime = Start + 100s;
  auto UnderTest = createTestedInstance(StopTime);

  for (auto &CFilter : UnderTest->_source_filters) {
    EXPECT_EQ(CFilter.second->get_stop_time(), StopTime);
  }
}

TEST_F(PartitionTest, SetStopTimePropagatesToFilters) {
  auto NewStopTime = Start + 12445s;
  auto UnderTest = createTestedInstance();
  UnderTest->setStopTime(NewStopTime);

  waitUntilDoneProcessing(UnderTest.get());
  for (auto &CFilter : UnderTest->_source_filters) {
    EXPECT_EQ(CFilter.second->get_stop_time(), NewStopTime);
  }
}

TEST_F(PartitionTest, IfSourceHashUnknownThenNotProcessed) {
  auto UnderTest = createTestedInstance();
  auto TestFilter = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr = TestFilter.get();
  REQUIRE_CALL(*TestFilterPtr, filter_message(_)).TIMES(1).RETURN(false);
  REQUIRE_CALL(*TestFilterPtr, has_finished()).TIMES(1).RETURN(false);
  UnderTest->_source_filters.clear();
  size_t SomeOtherHash{42};
  UnderTest->_source_filters.emplace_back(SomeOtherHash, std::move(TestFilter));
  setExtractorModule<zzzzFbReader>("zzzz");
  FileWriter::Msg Msg(SomeData.data(), SomeData.size());
  UnderTest->processMessage(Msg);
  EXPECT_EQ(int(UnderTest->MessagesProcessed), 0);
}

TEST_F(PartitionTest, IfSourceHashIsKnownThenItIsProcessed) {
  auto UnderTest = createTestedInstance();
  auto TestFilter = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr = TestFilter.get();
  UnderTest->_source_filters.clear();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter));
  REQUIRE_CALL(*TestFilterPtr, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr, has_finished()).TIMES(1).RETURN(false);
  setExtractorModule<zzzzFbReader>("zzzz");
  FileWriter::Msg Msg(SomeData.data(), SomeData.size());
  UnderTest->processMessage(Msg);
  EXPECT_EQ(int(UnderTest->MessagesProcessed), 1);
}

TEST_F(PartitionTest, FilterNotRemovedIfNotDone) {
  auto UnderTest = createTestedInstance();
  auto TestFilter = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr = TestFilter.get();
  auto OldSize = UnderTest->_source_filters.size();
  UnderTest->_source_filters.clear();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter));
  REQUIRE_CALL(*TestFilterPtr, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr, has_finished()).TIMES(1).RETURN(false);
  setExtractorModule<zzzzFbReader>("zzzz");
  FileWriter::Msg Msg(SomeData.data(), SomeData.size());
  UnderTest->processMessage(Msg);
  EXPECT_EQ(UnderTest->_source_filters.size(), OldSize);
}

TEST_F(PartitionTest, FilterIsRemovedWhenDone) {
  auto UnderTest = createTestedInstance();
  auto TestFilter = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr = TestFilter.get();
  auto OldSize = UnderTest->_source_filters.size();
  UnderTest->_source_filters.clear();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter));
  REQUIRE_CALL(*TestFilterPtr, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr, has_finished()).TIMES(1).RETURN(true);
  setExtractorModule<zzzzFbReader>("zzzz");
  FileWriter::Msg Msg(SomeData.data(), SomeData.size());
  UnderTest->processMessage(Msg);
  EXPECT_EQ(UnderTest->_source_filters.size(), OldSize - 1);
}

TEST_F(PartitionTest, MultipleFiltersAreRemovedWhenDone) {
  auto UnderTest = createTestedInstance();
  UnderTest->_source_filters.clear();

  auto TestFilter1 = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr1 = TestFilter1.get();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter1));
  REQUIRE_CALL(*TestFilterPtr1, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr1, has_finished()).TIMES(1).RETURN(true);

  auto TestFilter2 = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr2 = TestFilter2.get();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter2));
  REQUIRE_CALL(*TestFilterPtr2, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr2, has_finished()).TIMES(1).RETURN(true);
  EXPECT_EQ(UnderTest->_source_filters.size(), 2u);
  setExtractorModule<zzzzFbReader>("zzzz");
  FileWriter::Msg Msg(SomeData.data(), SomeData.size());
  UnderTest->processMessage(Msg);
  EXPECT_EQ(UnderTest->_source_filters.size(), 0u);
}

TEST_F(PartitionTest, PartitionHasNotFinishedIfAnyOfItsFiltersHaveNotFinished) {
  auto UnderTest = createTestedInstance();
  UnderTest->_source_filters.clear();

  auto TestFilter1 = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr1 = TestFilter1.get();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter1));
  REQUIRE_CALL(*TestFilterPtr1, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr1, has_finished()).TIMES(1).RETURN(true);

  auto TestFilter2 = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr2 = TestFilter2.get();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter2));
  REQUIRE_CALL(*TestFilterPtr2, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr2, has_finished()).TIMES(1).RETURN(false);

  FileWriter::MessageMetaData MetaData{
      1ms, RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 0, 0,
      "::some_topic::"};
  Kafka::MockConsumer::PollReturnType PollReturn{
      Kafka::PollStatus::Message,
      FileWriter::Msg{SomeData.data(), SomeData.size(), MetaData}};
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));

  setExtractorModule<zzzzFbReader>("zzzz");
  UnderTest->pollForMessage();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, HasNotFinishedAlt2) {
  auto UnderTest = createTestedInstance();
  UnderTest->_source_filters.clear();

  auto TestFilter1 = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr1 = TestFilter1.get();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter1));
  REQUIRE_CALL(*TestFilterPtr1, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr1, has_finished()).TIMES(1).RETURN(false);

  auto TestFilter2 = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr2 = TestFilter2.get();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter2));
  REQUIRE_CALL(*TestFilterPtr2, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr2, has_finished()).TIMES(1).RETURN(true);

  FileWriter::MessageMetaData MetaData{
      1ms, RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 0, 0,
      "::some_topic::"};
  Kafka::MockConsumer::PollReturnType PollReturn{
      Kafka::PollStatus::Message,
      FileWriter::Msg{SomeData.data(), SomeData.size(), MetaData}};
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));

  setExtractorModule<zzzzFbReader>("zzzz");
  UnderTest->pollForMessage();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, HasNotFinishedAlt3) {
  auto UnderTest = createTestedInstance();
  UnderTest->_source_filters.clear();

  auto TestFilter1 = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr1 = TestFilter1.get();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter1));
  REQUIRE_CALL(*TestFilterPtr1, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr1, has_finished()).TIMES(1).RETURN(false);

  FileWriter::MessageMetaData MetaData{
      1ms, RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 0, 0,
      "::some_topic::"};
  Kafka::MockConsumer::PollReturnType PollReturn{
      Kafka::PollStatus::Message,
      FileWriter::Msg{SomeData.data(), SomeData.size(), MetaData}};
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));

  setExtractorModule<zzzzFbReader>("zzzz");
  UnderTest->pollForMessage();
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, HasFinishedAlt1) {
  auto UnderTest = createTestedInstance();
  UnderTest->_source_filters.clear();

  auto TestFilter1 = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr1 = TestFilter1.get();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter1));
  REQUIRE_CALL(*TestFilterPtr1, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr1, has_finished()).TIMES(1).RETURN(true);

  FileWriter::MessageMetaData MetaData{
      1ms, RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 0, 0,
      "::some_topic::"};
  Kafka::MockConsumer::PollReturnType PollReturn{
      Kafka::PollStatus::Message,
      FileWriter::Msg{SomeData.data(), SomeData.size(), MetaData}};
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));

  setExtractorModule<zzzzFbReader>("zzzz");
  UnderTest->pollForMessage();
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(PartitionTest, PartitionHasFinishedIfAllItsFiltersHaveFinished) {
  auto UnderTest = createTestedInstance();
  UnderTest->_source_filters.clear();

  auto TestFilter1 = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr1 = TestFilter1.get();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter1));
  REQUIRE_CALL(*TestFilterPtr1, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr1, has_finished()).TIMES(1).RETURN(true);

  auto TestFilter2 = std::make_unique<SourceFilterStandInAlt>();
  auto TestFilterPtr2 = TestFilter2.get();
  UnderTest->_source_filters.emplace_back(UsedFilterHash,
                                          std::move(TestFilter2));
  REQUIRE_CALL(*TestFilterPtr2, filter_message(_)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestFilterPtr2, has_finished()).TIMES(1).RETURN(true);

  FileWriter::MessageMetaData MetaData{
      1ms, RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 0, 0,
      "::some_topic::"};
  Kafka::MockConsumer::PollReturnType PollReturn{
      Kafka::PollStatus::Message,
      FileWriter::Msg{SomeData.data(), SomeData.size(), MetaData}};
  REQUIRE_CALL(*Consumer, poll()).TIMES(1).LR_RETURN(std::move(PollReturn));

  setExtractorModule<zzzzFbReader>("zzzz");
  UnderTest->pollForMessage();
  EXPECT_TRUE(UnderTest->hasFinished());
}
