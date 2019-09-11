// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <gtest/gtest.h>
#include "Streamer.h"
#include "StreamMaster.h"

class ProducerStandIn : public KafkaW::Producer {
public:
  explicit ProducerStandIn(KafkaW::BrokerSettings &Settings)
      : Producer(Settings){};
  using Producer::ProducerID;
  using Producer::ProducerPtr;
};

class StreamMasterTests : public ::testing::Test {
public:
  void SetUp() override {
    FileWriterTask = std::make_unique<FileWriter::FileWriterTask>("Not Important", nullptr);
    FileWriterTask->setJobId(JobId);
    std::map<std::string, FileWriter::Streamer> Streamers;
    StreamMaster = std::make_unique<FileWriter::StreamMaster>(std::move(FileWriterTask), "ServiceID", nullptr, std::move(Streamers));

  };
  std::string JobId = "TestID";
  std::unique_ptr<FileWriter::FileWriterTask> FileWriterTask;
  std::unique_ptr<FileWriter::StreamMaster> StreamMaster;
};

TEST_F(StreamMasterTests, getJobIdReturnsCorrectValue) {
  ASSERT_EQ(JobId, StreamMaster->getJobId());
}

TEST_F(StreamMasterTests, whenConstructedIsNotRemovable) {
  ASSERT_FALSE(StreamMaster->isRemovable());
}

TEST_F(StreamMasterTests, whenRunningIsNotRemovable) {
  StreamMaster->start();

  ASSERT_FALSE(StreamMaster->isRemovable());
}
