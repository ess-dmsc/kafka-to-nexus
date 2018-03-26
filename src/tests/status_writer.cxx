#include <gtest/gtest.h>

#include "Status.h"
#include "StatusWriter.h"

using MessageInfo = FileWriter::Status::MessageInfo;
using StreamMasterInfo = FileWriter::Status::StreamMasterInfo;
using NLJSONWriter = FileWriter::Status::NLJSONWriter;

std::string getStringValue(const std::string &Key, nlohmann::json &Document) {
  try {
    if (auto x = find<std::string>(Key, Document)) {
      return x.inner();
    }
  } catch (std::exception &e) {
    std::cout << e.what() << '\n';
  }
  return "";
}
int getIntegerValue(const std::string &Key, nlohmann::json &Document) {
  try {
    if (auto x = find<int>(Key, Document)) {
      return x.inner();
    }
  } catch (std::exception &e) {
    std::cout << e.what() << '\n';
  }
  return -1;
}
double getDoubleValue(const std::string &Key, nlohmann::json &Document) {
  try {
    if (auto x = find<double>(Key, Document)) {
      return x.inner();
    }
  } catch (std::exception &e) {
    std::cout << e.what() << '\n';
  }
  return -1;
}

template <class T> class TD;

constexpr int n_messages{100};

TEST(StatusWriter, empty_writer_has_default_fields) {
  NLJSONWriter Writer;
  StreamMasterInfo sm;
  nlohmann::json json = Writer.get();
  EXPECT_EQ(getStringValue("type", json), "stream_master_status");
  EXPECT_EQ(getIntegerValue("next_message_eta_ms", json), 0);
  EXPECT_EQ(getIntegerValue("job_id", json), 0);
}

TEST(StatusWriter, add_empty_stream_master_info_uses_defaults) {
  FileWriter::Status::NLJSONWriter Writer;
  StreamMasterInfo sm;
  Writer.write(sm);
  nlohmann::json json = Writer.get();
  ASSERT_NO_THROW(json.at("stream_master"));

  EXPECT_EQ(getDoubleValue("Mbytes", json["stream_master"]), 0.0);
  EXPECT_EQ(getDoubleValue("errors", json["stream_master"]), 0.0);
  EXPECT_EQ(getDoubleValue("messages", json["stream_master"]), 0.0);
  EXPECT_EQ(getDoubleValue("runtime", json["stream_master"]), 0.0);
  EXPECT_EQ(getStringValue("state", json["stream_master"]), "Not Started");
}

TEST(StatusWriter, show_time_to_next_message) {
  StreamMasterInfo sm;
  sm.setTimeToNextMessage(std::chrono::milliseconds{1000});

  FileWriter::Status::NLJSONWriter Writer;
  Writer.write(sm);
  nlohmann::json json = Writer.get();

  EXPECT_EQ(getDoubleValue("next_message_eta_ms", json), 1000.0);
}

TEST(StatusWriter, add_message_updates_stream_master) {
  const size_t MessageSizeBytes = 1024;
  MessageInfo Message;
  StreamMasterInfo sm;
  Message.message(MessageSizeBytes);
  sm.add(Message);

  FileWriter::Status::NLJSONWriter Writer;

  Writer.write(sm);
  nlohmann::json json = Writer.get();

  // make sure that json structure is correct
  ASSERT_NO_THROW(json.at("stream_master"));

  EXPECT_EQ(getDoubleValue("Mbytes", json["stream_master"]),
            MessageSizeBytes * 1e-6);
  EXPECT_EQ(getDoubleValue("errors", json["stream_master"]), 0.0);
  EXPECT_EQ(getDoubleValue("messages", json["stream_master"]), 1.0);
  EXPECT_EQ(getStringValue("state", json["stream_master"]), "Not Started");
}

TEST(StatusWriter, add_error_updates_stream_master) {
  MessageInfo Message;
  StreamMasterInfo sm;

  Message.error();
  sm.add(Message);
  FileWriter::Status::NLJSONWriter Writer;
  Writer.write(sm);
  nlohmann::json json = Writer.get();

  // make sure that json structure is correct
  ASSERT_NO_THROW(json.at("stream_master"));

  EXPECT_EQ(getDoubleValue("Mbytes", json["stream_master"]), 0.0);
  EXPECT_EQ(getDoubleValue("errors", json["stream_master"]), 1.0);
  EXPECT_EQ(getDoubleValue("messages", json["stream_master"]), 0.0);
  EXPECT_EQ(getStringValue("state", json["stream_master"]), "Not Started");
}

TEST(StatusWriter, add_empty_message_info) {
  MessageInfo Message;
  std::chrono::milliseconds SinceLastMessage{1000};
  std::string Topic{"no-topic"};
  FileWriter::Status::NLJSONWriter Writer;

  Writer.write(Message, Topic, SinceLastMessage);

  nlohmann::json json = Writer.get();

  // make sure that json structure is correct
  ASSERT_NO_THROW(json.at("streamer"));
  ASSERT_NO_THROW(json["streamer"].at(Topic));
  ASSERT_NO_THROW(json["streamer"][Topic].at("status"));
  ASSERT_NO_THROW(json["streamer"][Topic].at("statistics"));

  EXPECT_EQ(
      getDoubleValue("average", json["streamer"][Topic]["statistics"]["size"]),
      0.0);
  EXPECT_EQ(getDoubleValue("stdandard_deviation",
                           json["streamer"][Topic]["statistics"]["size"]),
            0.0);
  EXPECT_EQ(getDoubleValue("average",
                           json["streamer"][Topic]["statistics"]["frequency"]),
            0.0);
  EXPECT_EQ(getDoubleValue("stdandard_deviation",
                           json["streamer"][Topic]["statistics"]["frequency"]),
            0.0);
  EXPECT_EQ(getDoubleValue("average",
                           json["streamer"][Topic]["statistics"]["throughput"]),
            0.0);
  EXPECT_EQ(getDoubleValue("stdandard_deviation",
                           json["streamer"][Topic]["statistics"]["throughput"]),
            0.0);
}

TEST(StatusWriter, add_valid_message_updates_streamer_info) {
  const size_t MessageSizeBytes = 1024;
  const double NumMessages = 1.0;
  MessageInfo Message;
  Message.message(MessageSizeBytes);

  std::chrono::milliseconds SinceLastMessage{1000};
  std::string Topic{"no-topic"};
  FileWriter::Status::NLJSONWriter Writer;
  Writer.write(Message, Topic, SinceLastMessage);

  nlohmann::json json = Writer.get();

  std::cout << json.dump(4) << "\n";

  // make sure that json structure is correct
  ASSERT_NO_THROW(json.at("streamer"));
  ASSERT_NO_THROW(json["streamer"].at(Topic));
  ASSERT_NO_THROW(json["streamer"][Topic].at("status"));
  ASSERT_NO_THROW(json["streamer"][Topic].at("statistics"));

  EXPECT_EQ(
      getDoubleValue("average", json["streamer"][Topic]["statistics"]["size"]),
      MessageSizeBytes / NumMessages * 1e-6);
  EXPECT_EQ(getDoubleValue("stdandard_deviation",
                           json["streamer"][Topic]["statistics"]["size"]),
            0.0);
  EXPECT_EQ(
      getDoubleValue("average",
                     json["streamer"][Topic]["statistics"]["frequency"]),
      NumMessages /
          std::chrono::duration_cast<std::chrono::seconds>(SinceLastMessage)
              .count());
  // EXPECT_EQ(getDoubleValue("stdandard_deviation",
  //                          json["streamer"][Topic]["statistics"]["frequency"]),
  //           0.0);
  EXPECT_EQ(
      getDoubleValue("average",
                     json["streamer"][Topic]["statistics"]["throughput"]),
      MessageSizeBytes * 1e-6 /
          std::chrono::duration_cast<std::chrono::seconds>(SinceLastMessage)
              .count());
  // EXPECT_EQ(getDoubleValue("stdandard_deviation",
  //                          json["streamer"][Topic]["statistics"]["throughput"]),
  //           0.0);
}
