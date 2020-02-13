// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandParser.h"
#include <chrono>
#include <gtest/gtest.h>
#include <pl72_run_start_generated.h>

class CommandParserHappyStartTests : public testing::Test {
public:
  FileWriter::StartCommandInfo StartInfo;

  std::string InstrumentNameInput = "TEST";
  std::string RunNameInput = "42";
  std::string NexusStructureInput = "{}";
  std::string JobIDInput = "qw3rty";
  std::string ServiceIDInput = "filewriter1";
  std::string BrokerInput = "somehost:1234";
  std::string FilenameInput = "a-dummy-name-01.h5";
  uint64_t StartTimeInput = 123456789000;
  uint64_t StopTimeInput = 123456790000;

  // cppcheck-suppress unusedFunction
  void SetUp() override {
    flatbuffers::FlatBufferBuilder Builder;

    const auto InstrumentNameOffset = Builder.CreateString(InstrumentNameInput);
    const auto RunIDOffset = Builder.CreateString(RunNameInput);
    const auto NexusStructureOffset = Builder.CreateString(NexusStructureInput);
    const auto JobIDOffset = Builder.CreateString(JobIDInput);
    const auto ServiceIDOffset = Builder.CreateString(ServiceIDInput);
    const auto BrokerOffset = Builder.CreateString(BrokerInput);
    const auto FilenameOffset = Builder.CreateString(FilenameInput);

    auto messageRunStart =
        CreateRunStart(Builder, StartTimeInput, StopTimeInput, RunIDOffset,
                       InstrumentNameOffset, NexusStructureOffset, JobIDOffset,
                       BrokerOffset, ServiceIDOffset, FilenameOffset);

    FinishRunStartBuffer(Builder, messageRunStart);
    auto MessageBuffer = Builder.Release();

    StartInfo = FileWriter::CommandParser::extractStartInformation(
        MessageBuffer.data(), MessageBuffer.size());
  }
};

TEST_F(CommandParserHappyStartTests, IfJobIDPresentThenExtractedCorrectly) {
  ASSERT_EQ(JobIDInput, StartInfo.JobID);
}

TEST_F(CommandParserHappyStartTests, IfFilenamePresentThenExtractedCorrectly) {
  ASSERT_EQ(FilenameInput, StartInfo.Filename);
}

TEST_F(CommandParserHappyStartTests, IfBrokerPresentThenExtractedCorrectly) {
  ASSERT_EQ(JobIDInput, StartInfo.BrokerInfo.HostPort);
  ASSERT_EQ(1234u, StartInfo.BrokerInfo.Port);
}

TEST_F(CommandParserHappyStartTests,
       IfNexusStructurePresentThenExtractedCorrectly) {
  ASSERT_EQ(NexusStructureInput, StartInfo.NexusStructure);
}

TEST_F(CommandParserHappyStartTests, IfStartPresentThenExtractedCorrectly) {
  ASSERT_EQ(std::chrono::milliseconds{StartTimeInput}, StartInfo.StartTime);
}

TEST_F(CommandParserHappyStartTests, IfStopPresentThenExtractedCorrectly) {
  ASSERT_EQ(std::chrono::milliseconds{StopTimeInput}, StartInfo.StopTime);
}

TEST_F(CommandParserHappyStartTests, IfServiceIdPresentThenExtractedCorrectly) {
  ASSERT_EQ(ServiceIDInput, StartInfo.ServiceID);
}

TEST(CommandParserSadStartTests, ThrowsIfNoJobID) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "broker": "localhost:9092",
  "nexus_structure": { }
})""");

  ASSERT_THROW(FileWriter::CommandParser::extractStartInformation(
                   nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserSadStartTests, ThrowsIfNoFileAttributes) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "broker": "localhost:9092",
  "nexus_structure": { }
})""");

  ASSERT_THROW(FileWriter::CommandParser::extractStartInformation(
                   nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserSadStartTests, ThrowsIfNoFilename) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "broker": "localhost:9092",
  "file_attributes": {
  },
  "nexus_structure": { }
})""");

  ASSERT_THROW(FileWriter::CommandParser::extractStartInformation(
                   nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserSadStartTests, ThrowsIfNoNexusStructure) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "broker": "localhost:9092",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  }
})""");

  ASSERT_THROW(FileWriter::CommandParser::extractStartInformation(
                   nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserSadStartTests, IfStopCommandPassedToStartMethodThenThrows) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_stop",
  "job_id": "qw3rty",
  "service_id": "filewriter1"
})""");

  ASSERT_THROW(FileWriter::CommandParser::extractStartInformation(
                   nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserSadStartTests, IfNoBrokerThenThrows) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "nexus_structure": { }
})""");

  ASSERT_THROW(FileWriter::CommandParser::extractStartInformation(
                   nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserSadStartTests, IfBrokerIsWrongFormThenThrows) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "broker": "1234:somehost",
  "nexus_structure": { }
})""");

  ASSERT_THROW(FileWriter::CommandParser::extractStartInformation(
                   nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserStartTests, IfNoStartTimeThenUsesSuppliedCurrentTime) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "broker": "localhost:9092",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "nexus_structure": { }
})""");

  auto FakeCurrentTime = std::chrono::milliseconds{987654321};

  auto StartInfo = FileWriter::CommandParser::extractStartInformation(
      nlohmann::json::parse(Command), FakeCurrentTime);

  ASSERT_EQ(FakeCurrentTime, StartInfo.StartTime);
}

TEST(CommandParserStartTests, IfNoStopTimeThenSetToZero) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "broker": "localhost:9092",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "nexus_structure": { }
})""");

  auto StartInfo = FileWriter::CommandParser::extractStartInformation(
      nlohmann::json::parse(Command));

  ASSERT_EQ(std::chrono::milliseconds::zero(), StartInfo.StopTime);
}

TEST(CommandParserStartTests, IfNoServiceIdThenIsBlank) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "broker": "localhost:9092",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "nexus_structure": { }
})""");

  auto StartInfo = FileWriter::CommandParser::extractStartInformation(
      nlohmann::json::parse(Command));

  ASSERT_EQ("", StartInfo.ServiceID);
}

TEST(CommandParserSadStopTests, IfNoJobIdThenThrows) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_stop"
})""");

  ASSERT_THROW(FileWriter::CommandParser::extractStopInformation(
                   nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserHappyStopTests, IfJobIdPresentThenExtractedCorrectly) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_stop",
  "job_id": "qw3rty"
})""");

  auto StopInfo = FileWriter::CommandParser::extractStopInformation(
      nlohmann::json::parse(Command));

  ASSERT_EQ("qw3rty", StopInfo.JobID);
}

TEST(CommandParserHappyStopTests, IfStopTimePresentThenExtractedCorrectly) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_stop",
  "job_id": "qw3rty",
  "stop_time": 123456790000
})""");

  auto StopInfo = FileWriter::CommandParser::extractStopInformation(
      nlohmann::json::parse(Command));

  ASSERT_EQ(std::chrono::milliseconds{123456790000}, StopInfo.StopTime);
}

TEST(CommandParserStopTests, IfNoStopTimeThenSetToZero) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_stop",
  "job_id": "qw3rty"
})""");

  auto StopInfo = FileWriter::CommandParser::extractStopInformation(
      nlohmann::json::parse(Command));

  ASSERT_EQ(std::chrono::milliseconds::zero(), StopInfo.StopTime);
}

TEST(CommandParserStopTests, IfNoServiceIdThenIsBlank) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_stop",
  "job_id": "qw3rty"
})""");

  auto StopInfo = FileWriter::CommandParser::extractStopInformation(
      nlohmann::json::parse(Command));

  ASSERT_EQ("", StopInfo.ServiceID);
}

TEST(CommandParserStopTests, IfServiceIdPresentThenExtractedCorrectly) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_stop",
  "job_id": "qw3rty",
  "service_id": "filewriter1"
})""");

  auto StopInfo = FileWriter::CommandParser::extractStopInformation(
      nlohmann::json::parse(Command));

  ASSERT_EQ("filewriter1", StopInfo.ServiceID);
}

TEST(CommandParserSadStopTests, IfStartCommandPassedToStopMethodThenThrows) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "nexus_structure": { }
})""");

  ASSERT_THROW(FileWriter::CommandParser::extractStopInformation(
                   nlohmann::json::parse(Command)),
               std::runtime_error);
}
