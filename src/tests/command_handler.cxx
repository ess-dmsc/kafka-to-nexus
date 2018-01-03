#include "../CommandHandler.h"
#include <fstream>
#include <gtest/gtest.h>
#include <rapidjson/filereadstream.h>
#include <sstream>

using namespace FileWriter;

namespace FileWriter {
std::string find_filename(rapidjson::Document const &);
std::string find_job_id(rapidjson::Document const &);
std::string find_broker(rapidjson::Document const &);

ESSTimeStamp find_time(rapidjson::Document const &, const std::string &);
} // namespace FileWriter

std::string parse_json_command(const std::string &filename) {
  std::ifstream t(filename);
  std::stringstream buffer;
  buffer << t.rdbuf();
  std::string cmd{buffer.str()};
  LOG(Sev::Dbg, "cmd: {}", cmd.c_str());
  return cmd;
}

class CommandHandler_Test : public ::testing::Test {

protected:
  virtual void SetUp() {
    auto s = std::move(parse_json_command("tests/msg-cmd-new-00.json"));
    new_00.Parse(s.c_str());
    s = std::move(parse_json_command("tests/msg-cmd-new-01.json"));
    new_01.Parse(s.c_str());
  }
  void test_filename_from_json() {
    ASSERT_EQ(FileWriter::find_filename(new_00), "a-dummy-name-00.h5");
  }
  void test_job_id_from_json() {
    ASSERT_EQ(FileWriter::find_job_id(new_00), "0000000000000000");
  }
  void test_broker_from_json() {
    ASSERT_EQ(FileWriter::find_broker(new_00), "192.168.10.11:9092");
  }
  void test_start_stop_time_from_json() {
    ASSERT_EQ(FileWriter::find_time(new_00, "start_time"),
              ESSTimeStamp{123456789});
    ASSERT_EQ(FileWriter::find_time(new_00, "stop_time"),
              ESSTimeStamp{123456790});
  }
  void test_missing_broker() {
    ASSERT_EQ(FileWriter::find_broker(new_01), "localhost:9092");
  }
  void test_missing_job_id() { ASSERT_EQ(FileWriter::find_job_id(new_01), ""); }
  void test_missing_start_stop_time() {
    ASSERT_EQ(FileWriter::find_time(new_01, "start"), ESSTimeStamp{0});
    ASSERT_EQ(FileWriter::find_time(new_01, "stop"), ESSTimeStamp{0});
  }

private:
  static rapidjson::Document new_00;
  static rapidjson::Document new_01;
};

rapidjson::Document CommandHandler_Test::new_00;
rapidjson::Document CommandHandler_Test::new_01;

TEST_F(CommandHandler_Test, test_found_filename_matches_expected) {
  CommandHandler_Test::test_filename_from_json();
}

TEST_F(CommandHandler_Test, test_found_job_id_matches_expected) {
  CommandHandler_Test::test_job_id_from_json();
}

TEST_F(CommandHandler_Test, test_found_broker_matches_expected) {
  CommandHandler_Test::test_broker_from_json();
}

TEST_F(CommandHandler_Test, test_found_start_stop_time_matche_expected) {
  CommandHandler_Test::test_start_stop_time_from_json();
}

TEST_F(CommandHandler_Test, test_missing_job_id_is_empty) {
  CommandHandler_Test::test_missing_job_id();
}

TEST_F(CommandHandler_Test, test_missing_broker_set_to_default) {
  CommandHandler_Test::test_missing_broker();
}

TEST_F(CommandHandler_Test, test_missing_start_stop_time_set_to_zero) {
  CommandHandler_Test::test_missing_start_stop_time();
}
