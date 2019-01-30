#include "CommandHandler.h"
#include "MainOpt.h"
#include "helper.h"
#include "json.h"
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>

using nlohmann::json;
using CLK = std::chrono::system_clock;

class CommandHandler_Testing : public testing::Test {
protected:
  static size_t
  FileWriterTasksSize(FileWriter::CommandHandler const &CommandHandler) {
    return CommandHandler.getNumberOfFileWriterTasks();
  }
};

TEST_F(CommandHandler_Testing, MissingStartTimeMeanStartNow) {

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
  FileWriter::CommandHandler CommandHandler(MainOpt, nullptr);
  CommandHandler.tryToHandle(std::make_unique<FileWriter::Msg>(
      FileWriter::Msg::owned(CommandString.data(), CommandString.size())));

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
  FileWriter::CommandHandler CommandHandler(MainOpt, nullptr);
  CommandHandler.tryToHandle(std::make_unique<FileWriter::Msg>(
      FileWriter::Msg::owned(CommandString.data(), CommandString.size())));

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
  FileWriter::CommandHandler CommandHandler(MainOpt, nullptr);
  CommandHandler.tryToHandle(std::make_unique<FileWriter::Msg>(
      FileWriter::Msg::owned(CommandString.data(), CommandString.size())));
  EXPECT_EQ(MainOpt.StreamerConfiguration.StartTimestamp.count(), 123456789);
  EXPECT_EQ(MainOpt.StreamerConfiguration.StopTimestamp.count(), 123456790);
  unlink("a-dummy-name-01.h5");
}

TEST_F(CommandHandler_Testing, MissingTimeHandleStopCommand) {
  std::string CommandString(
      R"""({"cmd":"FileWriter_stop","job_id": "xyzwt"})""");
  nlohmann::json Command = nlohmann::json::parse(CommandString);
  EXPECT_EQ(FileWriter::findTime(Command, "stop_time").count(), -1);
}

TEST_F(CommandHandler_Testing, FindTimeHandleStopCommand) {
  std::string CommandString(
      R"""({"cmd":"FileWriter_stop","job_id": "xyzwt","stop_time":987654321})""");
  nlohmann::json Command = nlohmann::json::parse(CommandString);
  EXPECT_EQ(FileWriter::findTime(Command, "stop_time").count(), 987654321);
}

TEST_F(CommandHandler_Testing, CatchExceptionOnAttemptToOverwriteFile) {
  std::ofstream ofs;
  ofs.open("tmp-dummy-hdf");
  ofs.close();
  MainOpt MainOpt;
  FileWriter::CommandHandler CommandHandler(MainOpt, nullptr);
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
  CommandHandler.tryToHandle(std::make_unique<FileWriter::Msg>(
      FileWriter::Msg::owned(CommandString.data(), CommandString.size())));
  // Assert that this command was not accepted by the command handler:
  ASSERT_EQ(CommandHandler_Testing::FileWriterTasksSize(CommandHandler),
            static_cast<size_t>(0));
  unlink("tmp-dummy-hdf");
}

void createFileWithOptionalSWMR(bool UseSWMR) {
  unlink("tmp_swmr_enable.h5");
  MainOpt MainOpt;
  FileWriter::CommandHandler CommandHandler(MainOpt, nullptr);
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
  CommandHandler.tryToHandle(std::make_unique<FileWriter::Msg>(
      FileWriter::Msg::owned(CommandString.data(), CommandString.size())));
  ASSERT_EQ(CommandHandler.getNumberOfFileWriterTasks(),
            static_cast<size_t>(1));
  auto &Task = CommandHandler.getFileWriterTaskByJobID("tmp_swmr_enable");
  ASSERT_EQ(Task->swmrEnabled(), UseSWMR);
  unlink("tmp_swmr_enable.h5");
}

TEST_F(CommandHandler_Testing, OpenFileInNonSWMRMode) {
  createFileWithOptionalSWMR(false);
}

TEST_F(CommandHandler_Testing, OpenFileInSWMRMode) {
  createFileWithOptionalSWMR(true);
}

TEST_F(CommandHandler_Testing, FormatNestedException) {
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

TEST_F(CommandHandler_Testing, faultyJsonLetsParserThrow) {
  ASSERT_THROW(FileWriter::parseOrThrow("{ this is not json }"),
               std::runtime_error);
}

TEST_F(CommandHandler_Testing, CreateHDFLinks) {
  std::string Filename("Test.CommandHandler_Testing.CreateHDFLinks");
  unlink(Filename.c_str());
  MainOpt MainOpt;
  FileWriter::CommandHandler CommandHandler(MainOpt, nullptr);
  auto Command = json::parse(R""(
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
        "type": "link",
        "name": "link_at_root",
        "target": "a_group/a_subgroup/value"
      },
      {
        "type": "group",
        "name": "extra_group",
        "children": [
          {
            "type": "link",
            "name": "some_link_to_value",
            "target": "../a_group/a_subgroup/value"
          },
          {
            "type": "link",
            "name": "some_link_to_a_group",
            "target": "../a_group/a_subgroup"
          },
          {
            "type": "link",
            "name": "some_absolute_link_to_a_dataset",
            "target": "/a_group/a_subgroup/value"
          }
        ]
      },
      {
        "type": "group",
        "name": "a_group",
        "children": [
          {
            "type": "group",
            "name": "a_subgroup",
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
    ]
  }
}
  )"");
  Command["file_attributes"]["file_name"] = Filename;
  auto CommandString = Command.dump();
  CommandHandler.tryToHandle(CommandString);
  ASSERT_EQ(CommandHandler_Testing::FileWriterTasksSize(CommandHandler),
            static_cast<size_t>(1));
  auto CommandStop = json::parse(R""(
{
  "cmd": "file_writer_tasks_clear_all",
  "recv_type": "FileWriter"
}
  )"");
  CommandString = CommandStop.dump();
  CommandHandler.tryToHandle(CommandString);
  ASSERT_EQ(CommandHandler_Testing::FileWriterTasksSize(CommandHandler),
            static_cast<size_t>(0));
  auto File = hdf5::file::open(Filename);
  File.root().get_group("a_group").get_group("a_subgroup").get_dataset("value");
  File.root().get_group("extra_group").get_dataset("some_link_to_value");
  File.root()
      .get_group("extra_group")
      .get_dataset("some_absolute_link_to_a_dataset");
  unlink(Filename.c_str());
}
