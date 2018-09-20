#include "StreamerOptions.h"

#include "json.h"
#include <gtest/gtest.h>

class StreamerOptionsTest : public ::testing::Test {
  virtual void SetUp() {}

public:
  void ConfigureRdKafkaWithJson(nlohmann::json const &Json) {
    Options.setRdKafkaOptions(Json["kafka"]);
  }
  void ConfigureStreamerWithJson(nlohmann::json const &Json) {
    Options.setStreamerOptions(Json["streamer"]);
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
  EXPECT_EQ(Options.BeforeStartTime.count(), BeforeStartTime.count());
  EXPECT_EQ(Options.NumMetadataRetry, NumMetadataRetry);
}

TEST_F(StreamerOptionsTest, rdkafkaEmptyJsonDoesnTChangeDefaults) {
  auto Json = nlohmann::json::parse("{ \"kafka\" : {} }");
  ConfigureRdKafkaWithJson(Json);
  FileWriter::StreamerOptions Defaults;
  CompareRdKafkaOptionsWith(Defaults);
}

TEST_F(StreamerOptionsTest, setRdkafkaOptionsFillsOptionsVector) {
  auto Json = nlohmann::json::parse(
      R"""({ "kafka": { "timeout.ms": 10, "api.version.request": "true", "metadata.broker": "localhost:9092"}})""");
  std::map<std::string, std::string> Strings;
  std::map<std::string, std::string> Integers;
  Integers["timeout.ms"] = 10;
  Strings["api.version.request"] = "true";
  Strings["metadata.broker"] = "localhost:9092";
  ConfigureRdKafkaWithJson(Json);
  CompareRdKafkaOptionsWith(Strings, Integers);
}

TEST_F(StreamerOptionsTest, setStreamerOptionsMatchesExpected) {
  auto Json = nlohmann::json::parse(
      "{\"streamer\" : {\"ms-before-start\" : 10, "
      "\"consumer-timeout-ms\" : 10, \"metadata-retry\" : 1}}");
  ConfigureStreamerWithJson(Json);
  CompareSteamerOptionsWith(std::chrono::milliseconds(10),
                            std::chrono::milliseconds(10), 1);
}
