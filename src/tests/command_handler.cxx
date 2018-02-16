#include "../CommandHandler.h"
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
  LOG(Sev::Debug, "cmd: {}", cmd.c_str());
  return cmd;
}

class CommandHandler_Test : public ::testing::Test {

protected:
  virtual void SetUp() {
    Cmd00 = parse_json_command("tests/msg-cmd-new-00.json");
    new_00.Parse(Cmd00.c_str());
    Cmd01 = parse_json_command("tests/msg-cmd-new-01.json");
    new_01.Parse(Cmd01.c_str());
  }
  void test_broker_from_json() {
    ASSERT_EQ(FileWriter::findBroker(Cmd00), "192.168.10.11:9092");
  }
  void test_missing_broker() {
    ASSERT_EQ(FileWriter::findBroker(Cmd01), "localhost:9092");
  }

private:
  std::string Cmd00;
  std::string Cmd01;
  static rapidjson::Document new_00;
  static rapidjson::Document new_01;
};

rapidjson::Document CommandHandler_Test::new_00;
rapidjson::Document CommandHandler_Test::new_01;

TEST_F(CommandHandler_Test, test_found_broker_matches_expected) {
  CommandHandler_Test::test_broker_from_json();
}

TEST_F(CommandHandler_Test, test_missing_broker_set_to_default) {
  CommandHandler_Test::test_missing_broker();
}
