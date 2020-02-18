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

class CommandParserHappyStartTests : public testing::Test {
public:
  FileWriter::StartCommandInfo StartInfo;
  std::string Good_Command{R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "broker": "somehost:1234",
  "start_time": 123456789000,
  "stop_time": 123456790000,
  "service_id": "filewriter1",
  "nexus_structure": { }
})"""};

  // cppcheck-suppress unusedFunction
  void SetUp() override {
    StartInfo = FileWriter::CommandParser::extractStartInformation(
        nlohmann::json::parse(Good_Command));
  }
};

TEST_F(CommandParserHappyStartTests, IfJobIDPresentThenExtractedCorrectly) {
  ASSERT_EQ("qw3rty", StartInfo.JobID);
}

TEST_F(CommandParserHappyStartTests, IfFilenamePresentThenExtractedCorrectly) {
  ASSERT_EQ("a-dummy-name-01.h5", StartInfo.Filename);
}

TEST_F(CommandParserHappyStartTests, IfBrokerPresentThenExtractedCorrectly) {
  ASSERT_EQ("somehost:1234", StartInfo.BrokerInfo.HostPort);
  ASSERT_EQ(1234u, StartInfo.BrokerInfo.Port);
}

TEST_F(CommandParserHappyStartTests,
       IfNexusStructurePresentThenExtractedCorrectly) {
  ASSERT_EQ("{}", StartInfo.NexusStructure);
}

TEST_F(CommandParserHappyStartTests, IfStartPresentThenExtractedCorrectly) {
  ASSERT_EQ(std::chrono::milliseconds{123456789000}, StartInfo.StartTime);
}

TEST_F(CommandParserHappyStartTests, IfStopPresentThenExtractedCorrectly) {
  ASSERT_EQ(std::chrono::milliseconds{123456790000}, StartInfo.StopTime);
}

TEST_F(CommandParserHappyStartTests, IfServiceIdPresentThenExtractedCorrectly) {
  ASSERT_EQ("filewriter1", StartInfo.ServiceID);
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
