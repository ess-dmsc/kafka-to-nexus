#include "CommandHandler.h"
#include "MainOpt.h"
#include "helper.h"
#include "json.h"
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>

using nlohmann::json;
using namespace FileWriter;
using CLK = std::chrono::system_clock;

class CommandHandler_Testing : public testing::Test {
protected:
  static size_t FileWriterTasksSize(CommandHandler const &CommandHandler) {
    return CommandHandler.getNumberOfFileWriterTasks();
  }
};

TEST_F(CommandHandler_Testing, MissingStratTimeMeanStartNow) {

  unlink("a-dummy-name-00.h5");
  std::string CommandString(R"""(
{
  "cmd": "FileWriter_new",
  "file_attributes": {
    "file_name": "a-dummy-name-00.h5"
  },
  "job_id": "qw3rty",
  "broker": "localhost:202020",
  "nexus_structure": { }
})""");

  MainOpt MainOpt;
  CommandHandler CommandHandler(MainOpt, nullptr);
  CommandHandler.handle(
      FileWriter::Msg::owned(CommandString.data(), CommandString.size()));

  std::chrono::milliseconds Now =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          CLK::now().time_since_epoch());
  // Allow 100ms tolerance
  EXPECT_NEAR(MainOpt.StreamerConfiguration.StartTimestamp.count(), Now.count(),
              100);
  unlink("a-dummy-name-00.h5");
}

TEST_F(CommandHandler_Testing, MissingStopTimeMeanNeverStop) {

  unlink("a-dummy-name-00.h5");
  std::string CommandString(R"""(
{
  "cmd": "FileWriter_new",
  "file_attributes": {
    "file_name": "a-dummy-name-00.h5"
  },
  "job_id": "qw3rty",
  "broker": "localhost:202020",
  "nexus_structure": { }
})""");

  MainOpt MainOpt;
  CommandHandler CommandHandler(MainOpt, nullptr);
  CommandHandler.handle(
      FileWriter::Msg::owned(CommandString.data(), CommandString.size()));

  EXPECT_FALSE(MainOpt.StreamerConfiguration.StopTimestamp.count() > 0);
  unlink("a-dummy-name-00.h5");
}

TEST_F(CommandHandler_Testing, UseFoundStartStopTime) {
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
  CommandHandler CommandHandler(MainOpt, nullptr);
  CommandHandler.handle(
      FileWriter::Msg::owned(CommandString.data(), CommandString.size()));
  EXPECT_EQ(MainOpt.StreamerConfiguration.StartTimestamp.count(), 123456789);
  EXPECT_EQ(MainOpt.StreamerConfiguration.StopTimestamp.count(), 123456790);
  unlink("a-dummy-name-01.h5");
}

TEST_F(CommandHandler_Testing, MissingTimeHandleStopCommand) {
  std::string CommandString(
      R"""({"cmd":"FileWriter_stop","job_id": "xyzwt"})""");
  nlohmann::json Command = nlohmann::json::parse(CommandString);
  EXPECT_EQ(findTime(Command, "stop_time").count(), -1);
}

TEST_F(CommandHandler_Testing, FindTimeHandleStopCommand) {
  std::string CommandString(
      R"""({"cmd":"FileWriter_stop","job_id": "xyzwt","stop_time":987654321})""");
  nlohmann::json Command = nlohmann::json::parse(CommandString);
  EXPECT_EQ(findTime(Command, "stop_time").count(), 987654321);
}

TEST_F(CommandHandler_Testing, CatchExceptionOnAttemptToOverwriteFile) {
  std::ofstream ofs;
  ofs.open("tmp-dummy-hdf");
  ofs.close();
  MainOpt MainOpt;
  CommandHandler CommandHandler(MainOpt, nullptr);
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
  CommandHandler.handle(
      FileWriter::Msg::owned(CommandString.data(), CommandString.size()));
  // Assert that this command was not accepted by the command handler:
  ASSERT_EQ(CommandHandler_Testing::FileWriterTasksSize(CommandHandler),
            static_cast<size_t>(0));
  unlink("tmp-dummy-hdf");
}

void createFileWithOptionalSWMR(bool UseSWMR) {
  unlink("tmp_swmr_enable.h5");
  MainOpt MainOpt;
  CommandHandler CommandHandler(MainOpt, nullptr);
  std::string CommandString = R"""(
{
  "cmd": "FileWriter_new",
  "file_attributes": {
    "file_name": "tmp_swmr_enable.h5"
  },
  "use_hdf_swmr": true,
  "job_id": "tmp_swmr_enable",
  "broker": "//localhost:202020",
  "nexus_structure": { "children": [] }
})""";
  auto Command = nlohmann::json::parse(CommandString);
  Command["use_hdf_swmr"] = UseSWMR;
  CommandString = Command.dump();
  CommandHandler.handle(
      FileWriter::Msg::owned(CommandString.data(), CommandString.size()));
  ASSERT_EQ(CommandHandler.getNumberOfFileWriterTasks(),
            static_cast<size_t>(1));
  auto &Task = CommandHandler.getFileWriterTaskByJobID("tmp_swmr_enable");
  ASSERT_EQ(Task->hdf_file.isSWMREnabled(), UseSWMR);
  unlink("tmp_swmr_enable.h5");
}

TEST_F(CommandHandler_Testing, OpenFileInNonSWMRMode) {
  createFileWithOptionalSWMR(false);
}

TEST_F(CommandHandler_Testing, OpenFileInSWMRMode) {
  createFileWithOptionalSWMR(true);
}
