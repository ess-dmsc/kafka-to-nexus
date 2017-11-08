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

  // void static test_source_from_json_00() {
  //   auto sources =
  //       FileWriter::find_source(CommandHandler_Test::new_00, nullptr);
  //   ASSERT_EQ(sources.size(), size_t{1});
  //   for (auto &s : sources) {
  //     ASSERT_EQ(s->_topic, "topic.with.multiple.sources");
  //     ASSERT_EQ(s->_source, "for_example_motor01");
  //     ASSERT_EQ(s->_broker, "");
  //     ASSERT_EQ(s->_hdf_path, "/entry-01/instrument-01/events-01");
  //   }
  // }
  // void static test_source_from_json_01() {
  //   auto sources = FileWriter::find_source(new_01, nullptr);
  //   ASSERT_EQ(sources.size(), size_t{2});
  //   ASSERT_EQ(sources[0]->_topic, "topic.with.multiple.sources");
  //   ASSERT_EQ(sources[0]->_source, "for_example_motor01");
  //   ASSERT_EQ(sources[0]->_broker, "");
  //   ASSERT_EQ(sources[0]->_hdf_path, "/entry-01/instrument-01/events-01");
  //   ASSERT_EQ(sources[1]->_topic, "topic.with.multiple.sources");
  //   ASSERT_EQ(sources[1]->_source, "for_example_temperature02");
  //   ASSERT_EQ(sources[1]->_broker, "");
  //   ASSERT_EQ(sources[1]->_hdf_path, "");
  // }

  static rapidjson::Document new_00;
  static rapidjson::Document new_01;
  static rapidjson::Document new_03;

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
rapidjson::Document CommandHandler_Test::new_00;
rapidjson::Document CommandHandler_Test::new_01;
rapidjson::Document CommandHandler_Test::new_03;

TEST_F(CommandHandler_Test, test_found_filename_matches_expected) {
  ASSERT_EQ(FileWriter::find_filename(new_00), "a-dummy-name.h5");
  ASSERT_EQ(FileWriter::find_filename(new_01), "tmp-new-01.h5");
  ASSERT_EQ(FileWriter::find_filename(new_03), "tmp-new-03.h5");
}

TEST_F(CommandHandler_Test, test_found_job_id_matches_expected) {
  ASSERT_EQ(FileWriter::find_job_id(new_00), "");
  ASSERT_EQ(FileWriter::find_job_id(new_01), "0000000000000001");
  ASSERT_EQ(FileWriter::find_job_id(new_03), "0000000000000003");
}

TEST_F(CommandHandler_Test, test_found_broker_matches_expected) {
  ASSERT_EQ(FileWriter::find_broker(new_00), "localhost:9092");
  ASSERT_EQ(FileWriter::find_broker(new_01), "localhost:9092");
  ASSERT_EQ(FileWriter::find_broker(new_03), "localhost:9092");
}

TEST_F(CommandHandler_Test,
       test_found_start_stop_time_matches_expected_or_set_to_zero) {
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

// TEST_F(CommandHandler_Test, test_found_source_matches_expected) {
//   CommandHandler_Test::test_source_from_json_00();
//   CommandHandler_Test::test_source_from_json_01();
// }
