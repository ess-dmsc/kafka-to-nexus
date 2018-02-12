#include "../CommandHandler.h"
#include <fstream>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <rapidjson/filereadstream.h>
#include <sstream>

using namespace FileWriter;

namespace FileWriter {

std::string find_broker(rapidjson::Document const &);

std::chrono::milliseconds find_time(rapidjson::Document const &,
                                    const std::string &);
} // namespace FileWriter

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
    auto s = parse_json_command("tests/msg-cmd-new-00.json");
    new_00.Parse(s.c_str());
    s = parse_json_command("tests/msg-cmd-new-01.json");
    new_01.Parse(s.c_str());
  }
  void test_broker_from_json() {
    ASSERT_EQ(FileWriter::find_broker(new_00), "192.168.10.11:9092");
  }
  void test_start_stop_time_from_json() {
    ASSERT_EQ(FileWriter::find_time(new_00, "start_time"),
              std::chrono::milliseconds{123456789});
    ASSERT_EQ(FileWriter::find_time(new_00, "stop_time"),
              std::chrono::milliseconds{123456790});
  }
  void test_missing_broker() {
    ASSERT_EQ(FileWriter::find_broker(new_01), "localhost:9092");
  }
  void test_missing_start_stop_time() {
    ASSERT_EQ(FileWriter::find_time(new_01, "start"),
              std::chrono::milliseconds{0});
    ASSERT_EQ(FileWriter::find_time(new_01, "stop"),
              std::chrono::milliseconds{0});
  }

private:
  static rapidjson::Document new_00;
  static rapidjson::Document new_01;
};

rapidjson::Document CommandHandler_Test::new_00;
rapidjson::Document CommandHandler_Test::new_01;

TEST_F(CommandHandler_Test, test_found_broker_matches_expected) {
  CommandHandler_Test::test_broker_from_json();
}

TEST_F(CommandHandler_Test, test_found_start_stop_time_matche_expected) {
  CommandHandler_Test::test_start_stop_time_from_json();
}

TEST_F(CommandHandler_Test, test_missing_broker_set_to_default) {
  CommandHandler_Test::test_missing_broker();
}

TEST_F(CommandHandler_Test, test_missing_start_stop_time_set_to_zero) {
  CommandHandler_Test::test_missing_start_stop_time();
}

TEST(CommandHandler, jsonmodern) {
  using std::string;
  using nlohmann::json;
  json d;
  d = json::parse(R"""({"x": 42})""");
  ASSERT_EQ(42, d.at("x"));
  // d.at("x").get<string>();
}
