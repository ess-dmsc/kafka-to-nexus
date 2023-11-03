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
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <memory>

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
  int produce(flatbuffers::DetachedBuffer const & /*Msg*/) override {
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
        Producer, std::move(ProducerTopic), TestStatusInformation);
  }

  std::unique_ptr<Status::StatusReporterBase> ReporterPtr;
  Status::ApplicationStatusInfo const TestStatusInformation =
      Status::ApplicationStatusInfo{std::chrono::milliseconds(100),
                                    "test_application",
                                    "test_version",
                                    "test_host_name",
                                    "test service name",
                                    "test_service_id",
                                    0};
};

TEST_F(StatusReporterTests, OnInitialisationValues) {
  ReporterPtr->setStatusGetter([=]() -> Status::JobStatusInfo { return {}; });
  ReporterPtr->setJSONStatisticsGenerator([](auto &) {});
  auto JSONReport = ReporterPtr->createJSONReport().dump();
  auto Report = ReporterPtr->createReport(JSONReport);
  auto StatusMsg = deserialiseStatusMessage(Report);

  ASSERT_EQ(StatusMsg.first.Filename, "");
  ASSERT_EQ(StatusMsg.first.StartTime, time_point(0ms));
  ASSERT_EQ(toMilliSeconds(StatusMsg.first.StopTime), 0);
}

TEST_F(StatusReporterTests, OnWritingInfoIsFilledOutCorrectly) {
  Status::JobStatusInfo const Info{Status::WorkerState::Writing, "1234",
                                   "file1.nxs", time_point(1234567890ms),
                                   time_point(19876543210ms)};
  ReporterPtr->setStatusGetter([=]() { return Info; });
  ReporterPtr->setJSONStatisticsGenerator([](auto &) {});

  auto JSONReport = ReporterPtr->createJSONReport().dump();
  auto Report = ReporterPtr->createReport(JSONReport);
  auto StatusMsg = deserialiseStatusMessage(Report);

  // Write job status information
  ASSERT_EQ(StatusMsg.first.JobId, Info.JobId);
  ASSERT_EQ(StatusMsg.first.Filename, Info.Filename);
  ASSERT_EQ(StatusMsg.first.StartTime, Info.StartTime);
  ASSERT_EQ(StatusMsg.first.StopTime, Info.StopTime);

  // Application status information
  ASSERT_EQ(StatusMsg.second.UpdateInterval,
            TestStatusInformation.UpdateInterval);
  ASSERT_EQ(StatusMsg.second.ApplicationName,
            TestStatusInformation.ApplicationName);
  ASSERT_EQ(StatusMsg.second.ApplicationVersion,
            TestStatusInformation.ApplicationVersion);
  ASSERT_EQ(StatusMsg.second.ServiceID, TestStatusInformation.ServiceID);
  ASSERT_EQ(StatusMsg.second.HostName, TestStatusInformation.HostName);
  ASSERT_EQ(StatusMsg.second.ProcessID, TestStatusInformation.ProcessID);
}
