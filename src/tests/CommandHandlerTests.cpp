// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandHandler.h"
#include "MainOpt.h"
#include "helper.h"
#include "json.h"
#include <gtest/gtest.h>

using nlohmann::json;
using CLK = std::chrono::system_clock;

TEST(CommandHandler_Testing, UseFoundStartStopTime) {
  unlink("a-dummy-name-01.h5");
  std::string CommandString(R"""(
{
  "cmd": "FileWriter_new",
  "file_attributes": {
    "file_name": "a-dummy-name-01.h5"
  },
  "job_id": "qw3rty",
  "broker": "localhost:202020",
  "start_time" : 123456789,
  "stop_time" : 123456790,
  "nexus_structure": { }
})""");

  MainOpt MainOpt;
  auto StreamsController = std::make_shared<FileWriter::StreamsController>();
  FileWriter::CommandHandler CommandHandler(MainOpt, StreamsController, nullptr);
  CommandHandler.tryToHandle(CommandString);
  EXPECT_EQ(MainOpt.StreamerConfiguration.StartTimestamp.count(), 123456789);
  EXPECT_EQ(MainOpt.StreamerConfiguration.StopTimestamp.count(), 123456790);
  unlink("a-dummy-name-01.h5");
}

TEST(CommandHandler_Testing, MissingTimeHandleStopCommand) {
  std::string CommandString(
      R"""({"cmd":"FileWriter_stop","job_id": "xyzwt"})""");
  nlohmann::json Command = nlohmann::json::parse(CommandString);
  EXPECT_EQ(FileWriter::findTime(Command, "stop_time").count(), -1);
}

TEST(CommandHandler_Testing, FindTimeHandleStopCommand) {
  std::string CommandString(
      R"""({"cmd":"FileWriter_stop","job_id": "xyzwt","stop_time":987654321})""");
  nlohmann::json Command = nlohmann::json::parse(CommandString);
  EXPECT_EQ(FileWriter::findTime(Command, "stop_time").count(), 987654321);
}

TEST(CommandHandler_Testing, displayMessageWhenCmdNotFound) {
  MainOpt MainOpt;
  auto StreamsController = std::make_shared<FileWriter::StreamsController>();
  FileWriter::CommandHandler CommandHandler(MainOpt, StreamsController, nullptr);
  std::string CommandString(
      R"""({"noCmd":"FileWriter_stop","job_id": "xyzwt","stop_time":987654321})""");
  testing::internal::CaptureStdout();
  CommandHandler.tryToHandle(CommandString);
  EXPECT_TRUE(testing::internal::GetCapturedStdout().find(
      "Can not extract 'cmd' from command."));
}

TEST(CommandHandler_Testing, displayMessageWhenCmdNotUnderstood) {
  MainOpt MainOpt;
  auto StreamsController = std::make_shared<FileWriter::StreamsController>();
  FileWriter::CommandHandler CommandHandler(MainOpt, StreamsController, nullptr);
  std::string CommandString(
      R"""({"cmd":"FileWriter_invalid_command","job_id": "xyzwt","stop_time":987654321})""");
  testing::internal::CaptureStdout();
  CommandHandler.tryToHandle(CommandString);
  EXPECT_TRUE(testing::internal::GetCapturedStdout().find(
      "Could not understand 'cmd' field of this command."));
}

TEST(CommandHandler_Testing, jobNotStartedIfItWillOverwriteFile) {
  std::ofstream ofs;
  ofs.open("tmp-dummy-hdf");
  ofs.close();
  MainOpt MainOpt;
  auto StreamsController = std::make_shared<FileWriter::StreamsController>();
  FileWriter::CommandHandler CommandHandler(MainOpt, StreamsController, nullptr);
  std::string CommandString(R"""(
{
  "cmd": "FileWriter_new",
  "file_attributes": {
    "file_name": "tmp-dummy-hdf"
  },
  "job_id": "vwa98nv983qn98snev",
  "broker": "//localhost:202020",
  "nexus_structure": {
    "children": [
      {
        "type": "group",
        "name": "some_group",
        "children": [
          {
            "type": "dataset",
            "name": "value",
            "values": 42.24
          }
        ]
      }
    ]
  }
})""");
  CommandHandler.tryToHandle(CommandString);
  // Assert that this command was not accepted by the command handler:
  ASSERT_FALSE(StreamsController->jobIDInUse("vwa98nv983qn98snev"));
  unlink("tmp-dummy-hdf");
}

TEST(CommandHandler_Testing, FormatNestedException) {
  try {
    try {
      throw std::runtime_error("2nd_level");
    } catch (...) {
      std::throw_with_nested(std::runtime_error("1st_level"));
    }
  } catch (std::exception const &E) {
    ASSERT_EQ(std::string("1st_level\n  2nd_level"),
              FileWriter::format_nested_exception(E));
  }
}
