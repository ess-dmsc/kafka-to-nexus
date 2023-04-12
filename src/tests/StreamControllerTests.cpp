// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FileWriterTask.h"
#include "Kafka/Producer.h"
#include "Metrics/MockSink.h"
#include "Stream/MessageWriter.h"
#include "StreamController.h"
#include "TimeUtility.h"
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

// class ProducerStandIn : public Kafka::Producer {
// public:
//   explicit ProducerStandIn(Kafka::BrokerSettings &Settings)
//       : Producer(Settings){};
//   using Producer::ProducerID;
//   using Producer::ProducerPtr;
// };

class StreamControllerMessageWriterStandIn : public Stream::MessageWriter {
public:
  explicit StreamControllerMessageWriterStandIn(
      std::function<void()> FlushFunction, duration FlushIntervalTime,
      Metrics::Registrar const &MetricReg)
      : Stream::MessageWriter(FlushFunction, FlushIntervalTime, MetricReg) {}
  // MAKE_CONST_MOCK0(nrOfWritesQueued, size_t(), override);

protected:
  void writeMsgImpl(WriterModule::Base *,
                    FileWriter::FlatbufferMessage const &) override {}
};

class StreamControllerStandIn : public FileWriter::StreamController {
public:
  StreamControllerStandIn(
      std::unique_ptr<FileWriter::FileWriterTask> FileWriterTask,
      FileWriter::StreamerOptions const &Settings,
      Metrics::Registrar const &Registrar,
      std::unique_ptr<Stream::MessageWriter> MessageWriter,
      MetaData::TrackerPtr const &Tracker)
      : StreamController::StreamController(std::move(FileWriterTask), Settings,
                                           Registrar, std::move(MessageWriter),
                                           Tracker) {}
  MAKE_CONST_MOCK0(nrOfWritesQueued, size_t(), override);
  MAKE_MOCK0(getTopicNames, void());
  MAKE_MOCK1(initStreams, void(std::set<std::string>));
  MAKE_MOCK0(pauseStreamers, void(), override);
  MAKE_MOCK0(resumeStreamers, void(), override);
};

class StreamControllerTests : public ::testing::Test {
public:
  void SetUp() override {
    FileWriterTask = std::make_unique<FileWriter::FileWriterTask>(
        TestRegistrar, std::make_shared<MetaData::Tracker>());
    FileWriterTask->setJobId(JobId);
    auto Registrar = Metrics::Registrar("some-app", {});
    auto StreamerOptions = FileWriter::StreamerOptions();
    MessageWriter = std::make_unique<StreamControllerMessageWriterStandIn>(
        [&]() { FileWriterTask->flushDataToFile(); },
        StreamerOptions.DataFlushInterval, Registrar.getNewRegistrar("stream"));
    StreamController = std::make_unique<StreamControllerStandIn>(
        std::move(FileWriterTask), StreamerOptions, Registrar,
        std::move(MessageWriter), std::make_shared<MetaData::Tracker>());
  };
  std::string JobId = "TestID";
  std::unique_ptr<FileWriter::FileWriterTask> FileWriterTask;
  std::unique_ptr<StreamControllerStandIn> StreamController;
  std::unique_ptr<Stream::MessageWriter> MessageWriter;

  std::unique_ptr<Metrics::Sink> TestSink{new Metrics::MockSink()};
  std::shared_ptr<Metrics::Reporter> TestReporter{
      new Metrics::Reporter(std::move(TestSink), 10ms)};
  std::vector<std::shared_ptr<Metrics::Reporter>> TestReporters{TestReporter};
  Metrics::Registrar TestRegistrar{"Test", TestReporters};
};

TEST_F(StreamControllerTests, getJobIdReturnsCorrectValue) {
  ASSERT_EQ(JobId, StreamController->getJobId());
}
