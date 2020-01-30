// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Status/StatusReporterBase.h"
#include "helpers/FakeStreamMaster.h"
#include "helpers/KafkaWMocks.h"
#include <gtest/gtest.h>
#include <memory>

using namespace FileWriter;

class ProducerStandIn : public KafkaW::Producer {
public:
  explicit ProducerStandIn(KafkaW::BrokerSettings &Settings)
      : Producer(Settings){};
  using Producer::ProducerID;
  using Producer::ProducerPtr;
};

class ProducerTopicStandIn : public KafkaW::ProducerTopic {
public:
  ProducerTopicStandIn(std::shared_ptr<KafkaW::Producer> ProducerPtr,
                       std::string const &TopicName)
      : ProducerTopic(std::move(ProducerPtr), std::move(TopicName)){};
  int produce(const std::string & /*MsgData*/) override { return 0; }
};

class StatusReporterTests : public ::testing::Test {
public:
  void SetUp() override {
    KafkaW::BrokerSettings BrokerSettings;
    std::shared_ptr<KafkaW::Producer> Producer =
        std::make_shared<ProducerStandIn>(BrokerSettings);

    std::unique_ptr<KafkaW::ProducerTopic> ProducerTopic =
        std::make_unique<ProducerTopicStandIn>(Producer, "SomeTopic");
    ReporterPtr = std::make_unique<Status::StatusReporterBase>(std::chrono::milliseconds{100}, std::move(ProducerTopic));
  }

  std::unique_ptr<Status::StatusReporterBase> ReporterPtr;
};

TEST_F(StatusReporterTests, OnInitialisationAllValuesHaveNonRunningValues) {
  auto Report = ReporterPtr->createReport();
  auto Json = nlohmann::json::parse(Report);

  ASSERT_EQ(Json["update_interval"], 100);
  ASSERT_EQ(Json["job_id"], "");
  ASSERT_EQ(Json["file_being_written"], "");
  ASSERT_EQ(Json["start_time"], 0);
  ASSERT_EQ(Json["stop_time"], 0);
}


TEST_F(StatusReporterTests, OnWritingFileIsFilledOutCorrectly) {
  Status::StatusInfo const Info{"1234", "file1.nxs", std::chrono::milliseconds(1234567890), std::chrono::milliseconds(19876543210)};
  ReporterPtr->updateStatusInfo(Info);

  auto Report = ReporterPtr->createReport();
  auto Json = nlohmann::json::parse(Report);

  ASSERT_EQ(Json["file_being_written"], Info.Filename);
}

