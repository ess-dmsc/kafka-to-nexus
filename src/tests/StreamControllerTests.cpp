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
#include "StreamController.h"
#include <gtest/gtest.h>

class ProducerStandIn : public Kafka::Producer {
public:
  explicit ProducerStandIn(Kafka::BrokerSettings &Settings)
      : Producer(Settings){};
  using Producer::ProducerID;
  using Producer::ProducerPtr;
};

class StreamControllerTests : public ::testing::Test {
public:
  void SetUp() override {
    FileWriterTask = std::make_unique<FileWriter::FileWriterTask>(
        std::make_shared<MetaData::Tracker>());
    FileWriterTask->setJobId(JobId);
    StreamController = std::make_unique<FileWriter::StreamController>(
        std::move(FileWriterTask), FileWriter::StreamerOptions(),
        Metrics::Registrar("some-app", {}),
        std::make_shared<MetaData::Tracker>());
  };
  std::string JobId = "TestID";
  std::unique_ptr<FileWriter::FileWriterTask> FileWriterTask;
  std::unique_ptr<FileWriter::StreamController> StreamController;
};

TEST_F(StreamControllerTests, getJobIdReturnsCorrectValue) {
  ASSERT_EQ(JobId, StreamController->getJobId());
}
