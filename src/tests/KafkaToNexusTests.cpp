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
#include "kafka-to-nexus.h"
#include <csignal>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

class FileWriterMasterMock : public FileWriter::Master {
public:
  FileWriterMasterMock(MainOpt &Config,
                       std::unique_ptr<Command::HandlerBase> Listener,
                       std::unique_ptr<Status::StatusReporterBase> Reporter,
                       Metrics::Registrar &Registrar)
      : FileWriter::Master(Config, std::move(Listener), std::move(Reporter),
                           &Registrar){};
  MAKE_MOCK0(stopNow, void(), override);
  MAKE_MOCK0(writingIsFinished, bool(), override);
};

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
             void(const std::filesystem::path &, nlohmann::json), override);
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

class KafkaToNexusTests : public ::testing::Test {
public:
  void SetUp() override {
    std::unique_ptr<Command::HandlerBase> TmpCmdHandler =
        std::make_unique<CommandHandlerStandIn>();
    auto CmdHandler =
        dynamic_cast<CommandHandlerStandIn *>(TmpCmdHandler.get());
    ALLOW_CALL(*CmdHandler, registerStartFunction(_));
    ALLOW_CALL(*CmdHandler, registerSetStopTimeFunction(_));
    ALLOW_CALL(*CmdHandler, registerStopNowFunction(_));
    ALLOW_CALL(*CmdHandler, registerIsWritingFunction(_));
    ALLOW_CALL(*CmdHandler, registerGetJobIdFunction(_));

    std::unique_ptr<Status::StatusReporterBase> TmpStatusReporter =
        std::make_unique<StatusReporterStandIn>();
    StatusReporter = // cppcheck-suppress danglingLifetime
        dynamic_cast<StatusReporterStandIn *>(TmpStatusReporter.get());
    ALLOW_CALL(*StatusReporter, setJSONMetaDataGenerator(_));

    MasterPtr = std::make_unique<FileWriterMasterMock>(
        Config, std::move(TmpCmdHandler), std::move(TmpStatusReporter),
        Registrar);
    MasterMockPtr = dynamic_cast<FileWriterMasterMock *>(MasterPtr.get());
  }
  MainOpt Config;
  Metrics::Registrar Registrar{"no_prefix", {}};
  void TearDown() override { MasterPtr.reset(); }
  StatusReporterStandIn *StatusReporter;
  std::unique_ptr<FileWriter::Master> MasterPtr;
  FileWriterMasterMock *MasterMockPtr;
};

TEST_F(KafkaToNexusTests, ShouldStopAlwaysTrueIfRunStateStopping) {
  std::atomic<RunStates> RunState{RunStates::Stopping};
  EXPECT_TRUE(shouldStop(MasterPtr, false, RunState));
  EXPECT_TRUE(shouldStop(MasterPtr, true, RunState));
}

TEST_F(KafkaToNexusTests, ShouldStopInFindTopicMode) {
  std::vector<RunStates> TrueStates = {RunStates::SIGINT_Received,
                                       RunStates::SIGHUP_Received};
  for (auto State : TrueStates) {
    std::atomic<RunStates> RunState{State};
    EXPECT_TRUE(shouldStop(MasterPtr, true, RunState))
        << "Failed for RunState: " << static_cast<int>(State);
  }
}

TEST_F(KafkaToNexusTests, ShouldStopAfterSIGINTIsTrueIfWritingFinished) {
  std::atomic<RunStates> RunState{RunStates::SIGINT_Received};
  REQUIRE_CALL(*MasterMockPtr, writingIsFinished()).RETURN(true);
  EXPECT_TRUE(shouldStop(MasterPtr, false, RunState));
}

TEST_F(KafkaToNexusTests,
       ShouldStopAfterSIGINTTransitionsToWaitingIfWritingNotFinished) {
  std::atomic<RunStates> RunState{RunStates::SIGINT_Received};
  REQUIRE_CALL(*MasterMockPtr, writingIsFinished()).RETURN(false);
  REQUIRE_CALL(*MasterMockPtr, stopNow());
  EXPECT_FALSE(shouldStop(MasterPtr, false, RunState));
  EXPECT_EQ(RunState, RunStates::SIGINT_Waiting);
}

TEST_F(KafkaToNexusTests, ShouldStopAfterSIGHUPIsTrueIfWritingFinished) {
  std::atomic<RunStates> RunState{RunStates::SIGHUP_Received};
  REQUIRE_CALL(*MasterMockPtr, writingIsFinished()).RETURN(true);
  EXPECT_TRUE(shouldStop(MasterPtr, false, RunState));
  EXPECT_EQ(RunState, RunStates::SIGINT_KafkaWait);
}

TEST_F(KafkaToNexusTests, ShouldStopAfterSIGHUPIsFalseIfWritingNotFinished) {
  std::atomic<RunStates> RunState{RunStates::SIGHUP_Received};
  REQUIRE_CALL(*MasterMockPtr, writingIsFinished()).RETURN(false);
  EXPECT_FALSE(shouldStop(MasterPtr, false, RunState));
  EXPECT_EQ(RunState, RunStates::SIGHUP_Received);
}

TEST_F(KafkaToNexusTests, FirstSIGINTMakesRunStateTransition) {
  std::atomic<RunStates> RunState;
  RunState = RunStates::Running;
  signal_handler(SIGINT, RunState);
  EXPECT_EQ(RunState, RunStates::SIGINT_Received);
  RunState = RunStates::SIGHUP_Received;
  signal_handler(SIGINT, RunState);
  EXPECT_EQ(RunState, RunStates::SIGINT_Received);
}

TEST_F(KafkaToNexusTests, SecondSIGINTMakesRunStateTransitionToStopping) {
  std::atomic<RunStates> RunState{RunStates::SIGINT_Received};
  signal_handler(SIGINT, RunState);
  EXPECT_EQ(RunState, RunStates::Stopping);
}

TEST_F(KafkaToNexusTests, SIGTERMMakesRunStateTransition) {
  std::atomic<RunStates> RunState{RunStates::Running};
  signal_handler(SIGTERM, RunState);
  EXPECT_EQ(RunState, RunStates::Stopping);
}

TEST_F(KafkaToNexusTests, SIGHUPMakesRunStateTransition) {
  std::atomic<RunStates> RunState{RunStates::Running};
  signal_handler(SIGHUP, RunState);
  EXPECT_EQ(RunState, RunStates::SIGHUP_Received);
}

TEST_F(KafkaToNexusTests, UnknownSignalIsIgnored) {
  std::atomic<RunStates> RunState{RunStates::Running};
  signal_handler(SIGUSR1, RunState);
  EXPECT_EQ(RunState, RunStates::Running);
}

TEST_F(KafkaToNexusTests, ReloadIgnoredIfThereWasAPendingRestart) {
  std::vector<RunStates> RestartStates = {
      RunStates::Stopping, RunStates::SIGINT_Received,
      RunStates::SIGINT_Waiting, RunStates::SIGINT_KafkaWait};
  for (auto State : RestartStates) {
    std::atomic<RunStates> InitialRunState{State};
    std::atomic<RunStates> RunState{State};
    signal_handler(SIGHUP, RunState);
    EXPECT_EQ(RunState, InitialRunState);
  }
}

TEST_F(KafkaToNexusTests, RestartHonouredIfThereWasAPendingReload) {
  std::vector<int> RestartSignals = {SIGINT, SIGTERM};
  for (auto RestartSignal : RestartSignals) {
    std::atomic<RunStates> RunState{RunStates::SIGHUP_Received};
    signal_handler(RestartSignal, RunState);
    EXPECT_NE(RunState, RunStates::SIGHUP_Received);
  }
}
