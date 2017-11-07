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

class CommandHandler_Test : public ::testing::Test {

protected:
  virtual void SetUp() {
    auto s = parse_impl("tests/msg-cmd-new-00.json");
    new_00.Parse(s.c_str());
    s = parse_impl("tests/msg-cmd-new-01.json");
    new_01.Parse(s.c_str());
    s = parse_impl("tests/msg-cmd-new-03.json");
    new_03.Parse(s.c_str());
  }

  rapidjson::Document new_00;
  rapidjson::Document new_01;
  rapidjson::Document new_03;

private:
  std::string parse_impl(const std::string &fname) const {
    std::ifstream t(fname);
    std::stringstream buffer;
    buffer << t.rdbuf();
    std::string cmd{buffer.str()};
    LOG(7, "cmd: {}", cmd.c_str());
    return cmd;
  }
};

TEST_F(CommandHandler_Test, filename) {
  ASSERT_EQ(FileWriter::find_filename(new_00), "a-dummy-name.h5");
  ASSERT_NE(FileWriter::find_filename(new_01), "");
  ASSERT_EQ(FileWriter::find_filename(new_01), "tmp-new-01.h5");
  ASSERT_NE(FileWriter::find_filename(new_03), "");
  ASSERT_EQ(FileWriter::find_filename(new_03), "tmp-new-03.h5");
}

TEST_F(CommandHandler_Test, job_id) {
  ASSERT_EQ(FileWriter::find_job_id(new_00), "");
  ASSERT_EQ(FileWriter::find_job_id(new_01), "0000000000000001");
  ASSERT_EQ(FileWriter::find_job_id(new_03), "0000000000000003");
}

TEST_F(CommandHandler_Test, broker) {
  ASSERT_EQ(FileWriter::find_broker(new_00), "localhost:9092");
  ASSERT_EQ(FileWriter::find_broker(new_01), "localhost:9092");
  ASSERT_EQ(FileWriter::find_broker(new_03), "localhost:9092");
}

TEST_F(CommandHandler_Test, start_stop) {
  ASSERT_EQ(FileWriter::find_time(new_00, "start_time"),
            FileWriter::ESSTimeStamp{123456789});
  ASSERT_EQ(FileWriter::find_time(new_00, "stop_time"),
            FileWriter::ESSTimeStamp{123456790});
  ASSERT_EQ(FileWriter::find_time(new_01, "start_time"),
            FileWriter::ESSTimeStamp{123456789});
  ASSERT_EQ(FileWriter::find_time(new_01, "stop_time"),
            FileWriter::ESSTimeStamp{0});
  ASSERT_EQ(FileWriter::find_time(new_03, "start_time"),
            FileWriter::ESSTimeStamp{0});
  ASSERT_EQ(FileWriter::find_time(new_03, "stop_time"),
            FileWriter::ESSTimeStamp{0});
}

