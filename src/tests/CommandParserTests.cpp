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

class CommandParserHappyTests : public testing::Test {
public:
  FileWriter::CommandParser Parser;
  std::string Good_Command{R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "broker": "somehost:1234",
  "start_time": 123456789,
  "stop_time": 123456790,
  "use_hdf_swmr": false,
  "abort_on_uninitialised_stream": true,
  "service_id": "filewriter1",
  "nexus_structure": { }
})"""};

  void SetUp() override {
    Parser.extractStartInformation(nlohmann::json::parse(Good_Command));
  }
};

TEST_F(CommandParserHappyTests, IfJobIDPresentThenExtractedCorrectly) {
  ASSERT_EQ("qw3rty", Parser.JobID);
}

TEST_F(CommandParserHappyTests, IfFilenamePresentThenExtractedCorrectly) {
  ASSERT_EQ("a-dummy-name-01.h5", Parser.Filename);
}

TEST_F(CommandParserHappyTests, IfSwmrPresentThenExtractedCorrectly) {
  ASSERT_FALSE(Parser.UseSwmr);
}

TEST_F(CommandParserHappyTests, IfBrokerPresentThenExtractedCorrectly) {
  ASSERT_EQ("somehost:1234", Parser.BrokerInfo.HostPort);
  ASSERT_EQ(1234u, Parser.BrokerInfo.Port);
}

TEST_F(CommandParserHappyTests, IfNexusStructurePresentThenExtractedCorrectly) {
  ASSERT_EQ("{}", Parser.NexusStructure);
}

TEST_F(CommandParserHappyTests, IfAbortPresentThenExtractedCorrectly) {
  ASSERT_TRUE(Parser.AbortOnStreamFailure);
}

TEST_F(CommandParserHappyTests, IfStartPresentThenExtractedCorrectly) {
  ASSERT_EQ(std::chrono::milliseconds{123456789}, Parser.StartTime);
}

TEST_F(CommandParserHappyTests, IfStopPresentThenExtractedCorrectly) {
  ASSERT_EQ(std::chrono::milliseconds{123456790}, Parser.StopTime);
}

TEST_F(CommandParserHappyTests, IfServiceIdPresentThenExtractedCorrectly) {
  ASSERT_EQ("filewriter1", Parser.ServiceID);
}

TEST(CommandParserSadTests, ThrowsIfNoJobID) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "broker": "localhost:9092",
  "start_time": 123456789,
  "stop_time": 123456790,
  "nexus_structure": { }
})""");

  FileWriter::CommandParser Parser;

  ASSERT_THROW(Parser.extractStartInformation(nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserSadTests, ThrowsIfNoFileAttributes) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "nexus_structure": { }
})""");

  FileWriter::CommandParser Parser;

  ASSERT_THROW(Parser.extractStartInformation(nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserSadTests, ThrowsIfNoFilename) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
  },
  "nexus_structure": { }
})""");

  FileWriter::CommandParser Parser;

  ASSERT_THROW(Parser.extractStartInformation(nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserSadTests, ThrowsIfNoNexusStructure) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  }
})""");

  FileWriter::CommandParser Parser;

  ASSERT_THROW(Parser.extractStartInformation(nlohmann::json::parse(Command)),
               std::runtime_error);
}

TEST(CommandParserTests, IfNoBrokerThenUsesDefault) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "nexus_structure": { }
})""");

  FileWriter::CommandParser Parser;
  Parser.extractStartInformation(nlohmann::json::parse(Command));

  ASSERT_TRUE(Parser.BrokerInfo.Port > 0u);
}

TEST(CommandParserTests, IfBrokerIsWrongFormThenUsesDefault) {
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

  FileWriter::CommandParser Parser;
  Parser.extractStartInformation(nlohmann::json::parse(Command));

  ASSERT_TRUE(Parser.BrokerInfo.Port > 0u);
}

TEST(CommandParserTests, IfNoStartTimeThenUsesSuppliedCurrentTime) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "nexus_structure": { }
})""");

  auto FakeCurrentTime = std::chrono::milliseconds{987654321};

  FileWriter::CommandParser Parser;
  Parser.extractStartInformation(nlohmann::json::parse(Command), FakeCurrentTime);

  ASSERT_EQ(FakeCurrentTime, Parser.StartTime);
}

TEST(CommandParserTests, IfNoStopTimeThenSetToZero) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "nexus_structure": { }
})""");

  FileWriter::CommandParser Parser;
  Parser.extractStartInformation(nlohmann::json::parse(Command));

  ASSERT_EQ(std::chrono::milliseconds::zero(), Parser.StopTime);
}

TEST(CommandParserTests, IfNoServiceIdThenIsBlank) {
  std::string Command(R"""(
{
  "cmd": "FileWriter_new",
  "job_id": "qw3rty",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "nexus_structure": { }
})""");

  FileWriter::CommandParser Parser;
  Parser.extractStartInformation(nlohmann::json::parse(Command));

  ASSERT_EQ("", Parser.ServiceID);
}
