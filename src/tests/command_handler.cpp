#include "CommandHandler.h"
#include <fstream>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <rapidjson/filereadstream.h>
#include <sstream>

using namespace FileWriter;

std::string parse_json_command(const std::string &filename) {
  std::ifstream t(filename);
  std::stringstream buffer;
  buffer << t.rdbuf();
  std::string cmd{buffer.str()};
  return cmd;
}

TEST(CommandHandler_Test, test_found_broker_matches_expected) {
  rapidjson::Document new_01;
  std::string Cmd01 =
      parse_json_command(std::string(TEST_DATA_PATH) + "/msg-cmd-new-01.json");
  new_01.Parse(Cmd01.c_str());
  ASSERT_EQ(FileWriter::findBroker(Cmd01), "localhost:9092");
}

TEST(CommandHandler_Test, test_missing_broker_set_to_default) {
  rapidjson::Document new_00;
  std::string Cmd00 =
      parse_json_command(std::string(TEST_DATA_PATH) + "/msg-cmd-new-00.json");
  new_00.Parse(Cmd00.c_str());
  ASSERT_EQ(FileWriter::findBroker(Cmd00), "192.168.10.11:9092");
}
