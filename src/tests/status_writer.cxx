#include <gtest/gtest.h>
#include <random>

#include "Status.h"
#include "StatusWriter.h"

#include "rapidjson/filewritestream.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/writer.h"

std::string getStringValue(const std::string &Key, nlohmann::json &Document) {
  if (auto x = find<std::string>(Key, Document)) {
    return x.inner();
  } else {
    return "";
  }
}

using MessageInfo = FileWriter::Status::MessageInfo;
using StreamMasterInfo = FileWriter::Status::StreamMasterInfo;
using StreamWriter = FileWriter::Status::JSONStreamWriter;

template <class T> class TD;

constexpr int n_messages{100};

TEST(StatusWriter, create_document) {
  StreamMasterInfo info;
  StreamWriter writer;
  auto value = writer.write(info);
  EXPECT_TRUE(value.size() > 0);
}

TEST(StatusWriter, create_report_streamers) {
  std::default_random_engine generator;
  std::normal_distribution<double> normal(0.0, 1.024e6);

  StreamMasterInfo info;
  const std::vector<std::string> topics{"first", "second", "third"};

  info.setTimeToNextMessage(std::chrono::milliseconds(2000));
  for (auto &t : topics) {
    MessageInfo mi;
    for (int i = 0; i < n_messages; ++i) {
      if (i % 10) {
        auto message_size = std::fabs(normal(generator));
        mi.message(message_size);
      } else {
        mi.error();
      }
    }
    info.add(mi);
  }

  StreamWriter writer;
  auto s = writer.write(info);

  // ASSERT_EQ(s,expect);
}

TEST(StatusWriter, create_empty_nl_writer) {
  FileWriter::Status::NLJSONWriter Writer;
  StreamMasterInfo sm;
  nlohmann::json json = Writer.write(sm);
  EXPECT_EQ(getStringValue("type", json), "stream_master_status");
}

TEST(StatusWriter, create_non_empty_nl_writer) {
  FileWriter::Status::NLJSONWriter Writer;
  StreamMasterInfo sm;
  nlohmann::json json = Writer.write(sm);
  std::cout << json.dump(4) << "\n";
}