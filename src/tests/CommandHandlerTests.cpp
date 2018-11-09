#include "CommandHandler.h"
#include "MainOpt.h"
#include "helper.h"
#include "json.h"
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>

using nlohmann::json;
using namespace FileWriter;

class CommandHandler_Testing : public testing::Test {
protected:
  static size_t FileWriterTasksSize(CommandHandler const &CommandHandler) {
    return CommandHandler.getNumberOfFileWriterTasks();
  }
};

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
  // Make sure that using 'tryToHandle' does not let the exception escape
  CommandHandler.tryToHandle(CommandString);
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
  CommandHandler.handle(CommandString);
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
              format_nested_exception(E));
  }
}

TEST_F(CommandHandler_Testing, faultyJsonLetsParserThrow) {
  ASSERT_THROW(parseOrThrow("{ this is not json }"), std::runtime_error);
}
