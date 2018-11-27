// This filename is chosen such that it shows up in searches after the
// case-sensitive flatbuffer schema identifier.

#include "AddReader.h"
#include "CommandHandler.h"
#include "FlatbufferMessage.h"
#include "MainOpt.h"
#include "Msg.h"
#include "helper.h"
#include "json.h"
#include "schemas/ev42/ev42_rw.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>
#include <memory>

using nlohmann::json;

class Schema_ev42 : public ::testing::Test {
public:
  void SetUp() override {
    try {
      FileWriter::FlatbufferReaderRegistry::Registrar<
          FileWriter::Schemas::ev42::FlatbufferReader>
          RegisterIt("ev42");
    } catch (...) {
    }
    try {
      FileWriter::HDFWriterModuleRegistry::Registrar<
          FileWriter::Schemas::ev42::HDFWriterModule>
          RegisterIt("ev42");
    } catch (...) {
    }
  }
};

TEST_F(Schema_ev42, UninitializedStreamOptionallyThrows) {
  using FileWriter::CommandHandler;
  using FileWriter::FlatbufferMessage;
  std::string Filename("Test.Schema_ev42.UninitializedStreamOptionallyThrows");
  unlink(Filename.c_str());
  MainOpt MainOpt;
  CommandHandler CommandHandler(MainOpt, nullptr);
  auto Command = json::parse(R""(
{
  "cmd": "FileWriter_new",
  "file_attributes": {
    "file_name": "tmp-dummy-hdf"
  },
  "job_id": "dummy",
  "broker": "//localhost:202020",
  "nexus_structure": {
    "children": [
      {
        "name": "some_nxlog",
        "type": "group",
        "children": [
          {
            "type": "stream",
            "stream": {
              "writer_module": "ev42",
              "topic": "dummy_topic",
              "source": "dummy_source_1"
            }
          },
          {
            "type": "stream",
            "stream": {
              "writer_module": "ev42",
              "topic": "dummy_topic",
              "source": "dummy_source_2"
            }
          }
        ]
      }
    ]
  }
}
  )"");
  Command["file_attributes"]["file_name"] = Filename;
  Command["job_id"] = Filename;
  Command["throw_on_uninitialized_stream"] = true;
  auto CommandString = Command.dump();
  ASSERT_THROW(CommandHandler.handle(CommandString), std::runtime_error);

  auto CommandStop = json::parse(R""(
{
  "cmd": "file_writer_tasks_clear_all",
  "recv_type": "FileWriter"
}
  )"");
  CommandString = CommandStop.dump();
  CommandHandler.handle(CommandString);
  unlink(Filename.c_str());
}
