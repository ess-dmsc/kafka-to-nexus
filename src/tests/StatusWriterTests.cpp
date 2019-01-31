#include <gtest/gtest.h>

#include "Status.h"
#include "StatusWriter.h"

using MessageInfo = FileWriter::Status::MessageInfo;
using StreamMasterInfo = FileWriter::Status::StreamMasterInfo;

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

uint64_t getUnsignedInteger64Value(const std::string &Key,
                                   nlohmann::json &Document) {
  try {
    if (auto x = find<uint64_t>(Key, Document)) {
      return x.inner();
    }
  } catch (std::exception &e) {
    std::cout << e.what() << '\n';
  }
  return 0u;
}

TEST(StatusWriter, emptyWriterHasDefaultFields) {
  FileWriter::Status::StatusWriter Writer;
  StreamMasterInfo sm;
  nlohmann::json json = nlohmann::json::parse(Writer.getJson());
  EXPECT_EQ(getStringValue("type", json), "stream_master_status");
  EXPECT_EQ(getIntegerValue("next_message_eta_ms", json), 0);
  EXPECT_EQ(getIntegerValue("job_id", json), 0);
  EXPECT_EQ(getUnsignedInteger64Value("timestamp", json), 0u);
}

int64_t getTimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
};

TEST(StatusWriter, addEmptyStreamMasterInfoUsesDefaults) {
  FileWriter::Status::StatusWriter Writer;
  StreamMasterInfo sm;
  Writer.write(sm);
  nlohmann::json json = nlohmann::json::parse(Writer.getJson());
  ASSERT_NO_THROW(json.at("stream_master"));

  EXPECT_EQ(getDoubleValue("Mbytes", json["stream_master"]), 0.0);
  EXPECT_EQ(getDoubleValue("errors", json["stream_master"]), 0.0);
  EXPECT_EQ(getDoubleValue("messages", json["stream_master"]), 0.0);
  EXPECT_LT(getDoubleValue("runtime", json["stream_master"]), 10.0);
  EXPECT_EQ(getStringValue("state", json["stream_master"]), "Not Started");
  EXPECT_NEAR(getUnsignedInteger64Value("timestamp", json), getTimestampMs(),
              1000);
}

TEST(StatusWriter, showTimeToNextMessage) {
  StreamMasterInfo sm;
  sm.setTimeToNextMessage(std::chrono::milliseconds{1000});

  FileWriter::Status::StatusWriter Writer;
  Writer.write(sm);
  nlohmann::json json = nlohmann::json::parse(Writer.getJson());

  EXPECT_EQ(getDoubleValue("next_message_eta_ms", json), 1000.0);
}

TEST(StatusWriter, addMessageUpdatesStreamMaster) {
  const size_t MessageSizeBytes = 1024;
  MessageInfo Message;
  StreamMasterInfo sm;
  Message.newMessage(MessageSizeBytes);
  sm.add(Message);

  FileWriter::Status::StatusWriter Writer;

  Writer.write(sm);
  nlohmann::json json = nlohmann::json::parse(Writer.getJson());

  // make sure that json structure is correct
  ASSERT_NO_THROW(json.at("stream_master"));

  EXPECT_EQ(getDoubleValue("Mbytes", json["stream_master"]),
            MessageSizeBytes * 1e-6);
  EXPECT_EQ(getDoubleValue("errors", json["stream_master"]), 0.0);
  EXPECT_EQ(getDoubleValue("messages", json["stream_master"]), 1.0);
  EXPECT_EQ(getStringValue("state", json["stream_master"]), "Not Started");
  EXPECT_NEAR(getUnsignedInteger64Value("timestamp", json), getTimestampMs(),
              1000);
}

TEST(StatusWriter, addErrorUpdatesStreamMaster) {
  MessageInfo Message;
  StreamMasterInfo sm;

  Message.error();
  sm.add(Message);
  FileWriter::Status::StatusWriter Writer;
  Writer.write(sm);
  nlohmann::json json = nlohmann::json::parse(Writer.getJson());

  // make sure that json structure is correct
  ASSERT_NO_THROW(json.at("stream_master"));

  EXPECT_EQ(getDoubleValue("Mbytes", json["stream_master"]), 0.0);
  EXPECT_EQ(getDoubleValue("errors", json["stream_master"]), 1.0);
  EXPECT_EQ(getDoubleValue("messages", json["stream_master"]), 0.0);
  EXPECT_EQ(getStringValue("state", json["stream_master"]), "Not Started");
  EXPECT_NEAR(getUnsignedInteger64Value("timestamp", json), getTimestampMs(),
              1000);
}

TEST(StatusWriter, addStreamEmptyMessageInfo) {
  MessageInfo Message;
  std::string Topic{"no-topic"};
  FileWriter::Status::StatusWriter Writer;

  Writer.write(Message, Topic);

  nlohmann::json json = nlohmann::json::parse(Writer.getJson());

  // make sure that json structure is correct
  ASSERT_NO_THROW(json.at("streamer"));
  ASSERT_NO_THROW(json["streamer"].at(Topic));
  ASSERT_NO_THROW(json["streamer"][Topic].at("rates"));

  EXPECT_EQ(getDoubleValue("average",
                           json["streamer"][Topic]["rates"]["message_size"]),
            0.0);
  EXPECT_EQ(getDoubleValue("standard_deviation",
                           json["streamer"][Topic]["rates"]["message_size"]),
            0.0);
  EXPECT_EQ(getDoubleValue("Mbytes", json["streamer"][Topic]["rates"]), 0.0);
  EXPECT_EQ(getIntegerValue("errors", json["streamer"][Topic]["rates"]), 0);
  EXPECT_EQ(getIntegerValue("messages", json["streamer"][Topic]["rates"]), 0);
}

TEST(StatusWriter, addStreamValidMessageUpdatesStreamerInfo) {
  const size_t MessageSizeBytes = 1024;
  const int NumMessages = 1;
  MessageInfo Message;
  Message.newMessage(MessageSizeBytes);

  std::string Topic{"no-topic"};
  FileWriter::Status::StatusWriter Writer;
  Writer.write(Message, Topic);

  nlohmann::json json = nlohmann::json::parse(Writer.getJson());

  // make sure that json structure is correct
  ASSERT_NO_THROW(json.at("streamer"));
  ASSERT_NO_THROW(json["streamer"].at(Topic));
  ASSERT_NO_THROW(json["streamer"][Topic].at("rates"));

  EXPECT_EQ(getDoubleValue("average",
                           json["streamer"][Topic]["rates"]["message_size"]),
            MessageSizeBytes / NumMessages * 1e-6);
  EXPECT_EQ(getDoubleValue("standard_deviation",
                           json["streamer"][Topic]["rates"]["message_size"]),
            0.0);
}

TEST(StatFunctions, messageSize) {
  const size_t NumMessages = 100;
  const size_t MessageBytes = 1024;
  MessageInfo Message;
  for (size_t i = 0; i < NumMessages; ++i) {
    Message.newMessage(MessageBytes * i);
  }
  std::pair<double, double> MessageSize = Message.messageSize();
  EXPECT_DOUBLE_EQ(MessageSize.first, 0.050688);
  EXPECT_DOUBLE_EQ(MessageSize.second, 0.0297077677833032);
}
