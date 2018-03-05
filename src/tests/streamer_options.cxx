#include "StreamerOptions.h"

#include "json.h"
#include <gtest/gtest.h>
#include <rapidjson/document.h>

namespace FileWriter {

class StreamerOptionsTest : public ::testing::Test {
  virtual void SetUp() {}

public:
  void ConfigureRdKafkaWithJson(const rapidjson::Document &optj) {
    ASSERT_TRUE(optj.HasMember("kafka"));
    Options.setRdKafkaOptions(&optj["kafka"]);
  }
  void ConfigureStreamerWithJson(const rapidjson::Document &optj) {
    ASSERT_TRUE(optj.HasMember("streamer"));
    Options.setStreamerOptions(&optj["streamer"]);
  }

  void CompareRdKafkaOptionsWith(const FileWriter::StreamerOptions &);

  void CompareRdKafkaOptionsWith(const std::map<std::string, std::string> &,
                                 const std::map<std::string, std::string> &);

  void CompareSteamerOptionsWith(const FileWriter::StreamerOptions &);

  void CompareSteamerOptionsWith(const std::chrono::milliseconds &,
                                 const std::chrono::milliseconds &,
                                 const int &);

protected:
  FileWriter::StreamerOptions Options;
};

void StreamerOptionsTest::CompareRdKafkaOptionsWith(
    const FileWriter::StreamerOptions &Other) {
  EXPECT_EQ(Options.Settings.ConfigurationIntegers,
            Other.Settings.ConfigurationIntegers);
  EXPECT_EQ(Options.Settings.ConfigurationStrings,
            Other.Settings.ConfigurationStrings);
  EXPECT_EQ(Options.ConsumerTimeout.count(), Other.ConsumerTimeout.count());
  EXPECT_EQ(Options.BeforeStartTime.count(), Other.BeforeStartTime.count());
  EXPECT_EQ(Options.NumMetadataRetry, Other.NumMetadataRetry);
}

void StreamerOptionsTest::CompareRdKafkaOptionsWith(
    const std::map<std::string, std::string> &Strings = {},
    const std::map<std::string, std::string> &Integers = {}) {
  EXPECT_EQ(Options.Settings.ConfigurationStrings, Strings);
}

void StreamerOptionsTest::CompareSteamerOptionsWith(
    const std::chrono::milliseconds &ConsumerTimeout,
    const std::chrono::milliseconds &BeforeStartTime,
    const int &NumMetadataRetry) {
  EXPECT_EQ(Options.ConsumerTimeout.count(), ConsumerTimeout.count());
  EXPECT_EQ(Options.BeforeStartTime.count(), BeforeStartTime.count());
  EXPECT_EQ(Options.NumMetadataRetry, NumMetadataRetry);
}

TEST_F(StreamerOptionsTest, rdkafka_empty_json_doesn_t_change_defaults) {
  rapidjson::Document optj;
  optj.Parse("{ \"kafka\" : {} }");

  ConfigureRdKafkaWithJson(optj);
  FileWriter::StreamerOptions Defaults;
  CompareRdKafkaOptionsWith(Defaults);
}

TEST_F(StreamerOptionsTest, set_rdkafka_options_fills_options_vector) {
  rapidjson::Document optj;
  std::map<std::string, std::string> Strings;
  std::map<std::string, std::string> Integers;
  Integers["timeout.ms"] = 10;
  Strings["api.version.request"] = "true";
  Strings["metadata.broker"] = "localhost:9092";
  optj.Parse("{ \"kafka\" : { \"timeout.ms\" : 10, \"api.version.request\" : "
             "\"true\", \"metadata.broker\" : \"localhost:9092\"}}");
  ConfigureRdKafkaWithJson(optj);
  CompareRdKafkaOptionsWith(Strings, Integers);
}

// TEST_F(StreamerOptionsTest, streamer_empty_json_doesn_t_change_defaults) {
//   rapidjson::Document optj;
//   optj.Parse("{ \"streamer\" : {} }");
//   ConfigureStreamerWithJson(optj);
//   FileWriter::StreamerOptions Defaults;
//   CompareSteamerOptionsWith(Defaults);
// }

TEST_F(StreamerOptionsTest, set_streamer_options_matches_expected) {
  rapidjson::Document optj;
  optj.Parse("{\"streamer\" : {\"ms-before-start\" : 10, "
             "\"consumer-timeout-ms\" : 10, \"metadata-retry\" : 1}}");
  ConfigureStreamerWithJson(optj);
  CompareSteamerOptionsWith(std::chrono::milliseconds(10),
                            std::chrono::milliseconds(10), 1);
}

} // namespace FileWriter
