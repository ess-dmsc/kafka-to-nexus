// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Status/StatusReporterBase.h"
#include "helpers/StatusHelpers.h"
#include <gtest/gtest.h>
#include <memory>
#include <nlohmann/json.hpp>
#include<flatbuffers/flatbuffers.h>

class ProducerStandIn : public Kafka::Producer {
public:
  explicit ProducerStandIn(Kafka::BrokerSettings &Settings)
      : Producer(Settings){};
  using Producer::ProducerID;
  using Producer::ProducerPtr;
};

class ProducerTopicStandIn : public Kafka::ProducerTopic {
public:
  ProducerTopicStandIn(std::shared_ptr<Kafka::Producer> ProducerPtr,
                       std::string TopicName)
      : ProducerTopic(std::move(ProducerPtr), std::move(TopicName)){};
  int produce(flatbuffers::DetachedBuffer const &/*Msg*/) override {
    return 0;
  }
};

class StatusReporterTests : public ::testing::Test {
public:
  void SetUp() override {
    Kafka::BrokerSettings BrokerSettings;
    std::shared_ptr<Kafka::Producer> Producer =
        std::make_shared<ProducerStandIn>(BrokerSettings);

    std::unique_ptr<Kafka::ProducerTopic> ProducerTopic =
        std::make_unique<ProducerTopicStandIn>(Producer, "SomeTopic");
    ReporterPtr = std::make_unique<Status::StatusReporterBase>(
        std::chrono::milliseconds{100}, "ServiceId", std::move(ProducerTopic));
  }

  std::unique_ptr<Status::StatusReporterBase> ReporterPtr;
};

TEST_F(StatusReporterTests, OnInitialisationAllValuesHaveNonRunningValues) {
  auto JSONReport = ReporterPtr->createJSONReport();
  auto Report = ReporterPtr->createReport(JSONReport);
  auto StatusMsg = deserialiseStatusMessage(Report);

  ASSERT_EQ(StatusMsg.JobId, "");
  ASSERT_EQ(StatusMsg.Filename, "");
  ASSERT_EQ(StatusMsg.StartTime.count(), 0);
  ASSERT_EQ(StatusMsg.StopTime.count(), 0);
}

TEST_F(StatusReporterTests, OnWritingInfoIsFilledOutCorrectly) {
  Status::StatusInfo const Info{"1234", "file1.nxs", 1234567890ms,
                                time_point(19876543210ms)};
  ReporterPtr->updateStatusInfo(Info);

  auto const Report = ReporterPtr->createJSONReport();
  auto const Json = nlohmann::json::parse(Report);

  ASSERT_EQ(Json["job_id"], Info.JobId);
  ASSERT_EQ(Json["file_being_written"], Info.Filename);
  ASSERT_EQ(Json["start_time"], Info.StartTime.count());
  ASSERT_EQ(Json["stop_time"], toMilliSeconds(Info.StopTime));
}

TEST_F(StatusReporterTests, UpdatingStoptimeUpdatesReport) {
  auto const StopTime = 1234567890ms;
  ReporterPtr->updateStopTime(StopTime);

  auto const Report = ReporterPtr->createJSONReport();
  auto const Json = nlohmann::json::parse(Report);

  ASSERT_EQ(Json["stop_time"], StopTime.count());
}

TEST_F(StatusReporterTests, ResettingValuesClearsValuesSet) {
  Status::StatusInfo const Info{"1234", "file1.nxs", 1234567890ms,
                                time_point(19876543210ms)};
  ReporterPtr->updateStatusInfo(Info);

  ReporterPtr->resetStatusInfo();
  auto Report = ReporterPtr->createJSONReport();
  auto Json = nlohmann::json::parse(Report);

  ASSERT_EQ(Json["job_id"], "");
  ASSERT_EQ(Json["file_being_written"], "");
  ASSERT_EQ(Json["start_time"], 0);
  ASSERT_EQ(Json["stop_time"], 0);
}
