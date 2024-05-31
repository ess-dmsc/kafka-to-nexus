// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Master.h"
#include "Status/StatusReporter.h"
#include "StreamController.h"
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <memory>
#include <trompeloeil.hpp>

class CommandHandlerStandIn : public Command::HandlerBase {
public:
  MAKE_MOCK1(registerStartFunction, void(Command::StartFuncType), override);
  MAKE_MOCK1(registerSetStopTimeFunction, void(Command::StopTimeFuncType),
             override);
  MAKE_MOCK1(registerStopNowFunction, void(Command::StopNowFuncType), override);
  MAKE_MOCK1(registerIsWritingFunction, void(Command::IsWritingFuncType),
             override);
  MAKE_MOCK1(registerGetJobIdFunction, void(Command::GetJobIdFuncType),
             override);

  MAKE_MOCK2(sendHasStoppedMessage,
             void(const std::filesystem::path &, std::string const &), override);
  MAKE_MOCK3(sendErrorEncounteredMessage,
             void(const std::string &, const std::string &,
                  const std::string &),
             override);

  MAKE_MOCK0(loopFunction, void(), override);
};

class StatusReporterStandIn : public Status::StatusReporterBase {
public:
  StatusReporterStandIn()
      : Status::StatusReporterBase(std::shared_ptr<Kafka::Producer>{}, {}, {}) {
  }
  MAKE_MOCK1(useAlternativeStatusTopic, void(std::string const &), override);
  MAKE_MOCK0(revertToDefaultStatusTopic, void(), override);
  MAKE_CONST_MOCK0(getStopTime, time_point(), override);
  MAKE_CONST_MOCK1(createReport,
                   flatbuffers::DetachedBuffer(std::string const &), override);
  MAKE_CONST_MOCK0(createJSONReport, nlohmann::json(), override);
  MAKE_MOCK1(setJSONMetaDataGenerator, void(Status::JsonGeneratorFuncType),
             override);
};

using trompeloeil::_;

class MasterTest : public ::testing::Test {
public:
  void SetUp() override {
    std::unique_ptr<Command::HandlerBase> TmpCmdHandler =
        std::make_unique<CommandHandlerStandIn>();
    auto CmdHandler =
        dynamic_cast<CommandHandlerStandIn *>(TmpCmdHandler.get());

    REQUIRE_CALL(*CmdHandler, registerStartFunction(_)).TIMES(1);
    REQUIRE_CALL(*CmdHandler, registerSetStopTimeFunction(_)).TIMES(1);
    REQUIRE_CALL(*CmdHandler, registerStopNowFunction(_)).TIMES(1);
    REQUIRE_CALL(*CmdHandler, registerIsWritingFunction(_)).TIMES(1);
    REQUIRE_CALL(*CmdHandler, registerGetJobIdFunction(_)).TIMES(1);

    std::unique_ptr<Status::StatusReporterBase> TmpStatusReporter =
        std::make_unique<StatusReporterStandIn>();
    StatusReporter = // cppcheck-suppress danglingLifetime
        dynamic_cast<StatusReporterStandIn *>(TmpStatusReporter.get());

    REQUIRE_CALL(*StatusReporter, setJSONMetaDataGenerator(_)).TIMES(1);

    UnderTest = std::make_unique<FileWriter::Master>(
        Config, std::move(TmpCmdHandler), std::move(TmpStatusReporter),
        std::make_unique<Metrics::Registrar>("some_prefix"));

    std::filesystem::path FullFilePath = Config.getHDFOutputPrefix();
    FullFilePath.append(StartCmd.Filename);
    if (!StartCmd.Filename.empty() && std::filesystem::exists(FullFilePath)) {
      std::filesystem::remove(FullFilePath);
    }
    FullFilePath =
        std::filesystem::path(Config.getHDFOutputPrefix()) /
        std::filesystem::path(StartCmdAbsoluteFilename).relative_path();
    if (std::filesystem::exists(FullFilePath)) {
      std::filesystem::remove(FullFilePath);
    }
    if (!std::filesystem::exists(FullFilePath.parent_path())) {
      std::filesystem::create_directory(FullFilePath.parent_path());
    }
  }
  MainOpt Config;

  void TearDown() override { UnderTest.reset(); }
  StatusReporterStandIn *StatusReporter;
  std::unique_ptr<FileWriter::Master> UnderTest;
  time_point StartTime{system_clock::now()};
  Command::StartInfo StartCmd{"job_id",
                              "some_file_name",
                              R"({"nexus_structure":5})",
                              R"({"meta_data":54})",
                              StartTime,
                              StartTime + 50s,
                              "control_topic"};
  std::string StartCmdAbsoluteFilename{
      std::filesystem::temp_directory_path().append("some_file_name")};
  Command::StartInfo StartCmdAbsolute{"job_id",
                                      StartCmdAbsoluteFilename,
                                      R"({"nexus_structure":5})",
                                      R"({"meta_data":54})",
                                      StartTime,
                                      StartTime + 50s,
                                      "control_topic"};
};

TEST_F(MasterTest, Init) {
  // Do nothing extra here, its all done in the SetUp()-function
}

TEST_F(MasterTest, WorkerStateStartsAtIdle) {
  EXPECT_EQ(UnderTest->getCurrentState(), Status::WorkerState::Idle);
  EXPECT_EQ(UnderTest->getCurrentStateMetric(), 0);
}

TEST_F(MasterTest, DestinationFilenameFromRelativePath) {
  REQUIRE_CALL(*StatusReporter,
               useAlternativeStatusTopic(StartCmd.ControlTopic));
  UnderTest->startWriting(StartCmd);
  std::filesystem::path FullFilePath =
      std::filesystem::path(Config.getHDFOutputPrefix()) / StartCmd.Filename;
  EXPECT_EQ(UnderTest->getCurrentFilePath(), FullFilePath);
}

TEST_F(MasterTest, DestinationFilenameFromAbsolutePath) {
  REQUIRE_CALL(*StatusReporter,
               useAlternativeStatusTopic(StartCmdAbsolute.ControlTopic));
  UnderTest->startWriting(StartCmdAbsolute);
  std::filesystem::path FullFilePath = std::filesystem::path(
      Config.getHDFOutputPrefix() + StartCmdAbsolute.Filename);
  EXPECT_EQ(UnderTest->getCurrentFilePath(), FullFilePath);
}

TEST_F(MasterTest, StartWritingSuccess) {
  REQUIRE_CALL(*StatusReporter,
               useAlternativeStatusTopic(StartCmd.ControlTopic))
      .TIMES(1);
  UnderTest->startWriting(StartCmd);
  EXPECT_EQ(UnderTest->getCurrentState(), Status::WorkerState::Writing);
  EXPECT_EQ(UnderTest->getCurrentStateMetric(), 1);
}

TEST_F(MasterTest, StartWritingFailureWhenWriting) {
  REQUIRE_CALL(*StatusReporter,
               useAlternativeStatusTopic(StartCmd.ControlTopic))
      .TIMES(1);
  UnderTest->startWriting(StartCmd);
  ASSERT_EQ(UnderTest->getCurrentState(), Status::WorkerState::Writing);
  EXPECT_EQ(UnderTest->getCurrentStateMetric(), 1);
  EXPECT_THROW(UnderTest->startWriting(StartCmd), std::runtime_error);
}

TEST_F(MasterTest, SetStopTimeFailsWhenIdle) {
  EXPECT_THROW(UnderTest->setStopTime(system_clock::now()), std::runtime_error);
}

TEST_F(MasterTest, SetStopTimeSuccess) {
  REQUIRE_CALL(*StatusReporter,
               useAlternativeStatusTopic(StartCmd.ControlTopic))
      .TIMES(1);
  UnderTest->startWriting(StartCmd);
  auto NewStopTime = StartTime + 5s;
  ALLOW_CALL(*StatusReporter, getStopTime()).RETURN(StartCmd.StopTime);
  UnderTest->setStopTime(NewStopTime);
  EXPECT_EQ(UnderTest->getStopTime(), NewStopTime);
}

TEST_F(MasterTest, SetStopTimeInThePastSuccess) {
  REQUIRE_CALL(*StatusReporter,
               useAlternativeStatusTopic(StartCmd.ControlTopic))
      .TIMES(1);
  UnderTest->startWriting(StartCmd);
  auto NewStopTime = StartTime - 5s;
  ALLOW_CALL(*StatusReporter, getStopTime()).RETURN(StartCmd.StopTime);
  UnderTest->setStopTime(NewStopTime);
  EXPECT_EQ(UnderTest->getStopTime(), NewStopTime);
}

TEST_F(MasterTest, SetStopTimeFailureDueToStopTimePassed) {
  REQUIRE_CALL(*StatusReporter,
               useAlternativeStatusTopic(StartCmd.ControlTopic))
      .TIMES(1);
  UnderTest->startWriting(StartCmd);
  auto NewStopTime1 = StartTime - 5s;
  ALLOW_CALL(*StatusReporter, getStopTime()).RETURN(StartCmd.StopTime);
  UnderTest->setStopTime(NewStopTime1);

  auto NewStopTime2 = StartTime + 20s;
  ALLOW_CALL(*StatusReporter, getStopTime()).RETURN(NewStopTime1);
  EXPECT_THROW(UnderTest->setStopTime(NewStopTime2), std::runtime_error);
}

TEST_F(MasterTest, StopNowFailureDueToIdle) {
  EXPECT_THROW(UnderTest->stopNow(), std::runtime_error);
}

TEST_F(MasterTest, StopNowSuccess) {
  REQUIRE_CALL(*StatusReporter,
               useAlternativeStatusTopic(StartCmd.ControlTopic))
      .TIMES(1);
  UnderTest->startWriting(StartCmd);
  EXPECT_NO_THROW(UnderTest->stopNow());
}

TEST_F(MasterTest, StopNowSuccessWhenStopTimePassed) {
  REQUIRE_CALL(*StatusReporter,
               useAlternativeStatusTopic(StartCmd.ControlTopic))
      .TIMES(1);

  UnderTest->startWriting(StartCmd);
  auto NewStopTime1 = StartTime - 5s;

  ALLOW_CALL(*StatusReporter, getStopTime()).RETURN(StartCmd.StopTime);
  UnderTest->setStopTime(NewStopTime1);

  UnderTest->stopNow();
}
