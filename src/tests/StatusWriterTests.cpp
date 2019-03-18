#include <gtest/gtest.h>

#include "Status.h"
#include "StatusWriter.h"

using MessageInfo = FileWriter::Status::MessageInfo;
using StreamMasterInfo = FileWriter::Status::StreamMasterInfo;

/// Helper method to extract values.
template <typename T>
T getValue(const std::string &Key, nlohmann::json &Document) {

  if (auto x = find<T>(Key, Document)) {
    return x.inner();
  }

  throw std::runtime_error("Could not get value");
}

TEST(StatusWriter, emptyWriterHasDefaultFields) {
  FileWriter::Status::StatusWriter Writer;
  StreamMasterInfo sm;
  nlohmann::json json = nlohmann::json::parse(Writer.getJson());
  std::string temp = json.dump();
  EXPECT_EQ(getValue<std::string>("type", json), "stream_master_status");
  EXPECT_EQ(getValue<int>("next_message_eta_ms", json), 0);
  EXPECT_EQ(getValue<int>("job_id", json), 0);
}

int64_t getTimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
};

TEST(StatusWriter, addEmptyStreamMasterInfoUsesDefaults) {
  FileWriter::Status::StatusWriter Writer;
  StreamMasterInfo sm;
  Writer.write(sm);
  nlohmann::json json = nlohmann::json::parse(Writer.getJson());
  ASSERT_NO_THROW(json.at("stream_master"));

  EXPECT_EQ(getValue<double>("Mbytes", json["stream_master"]), 0.0);
  EXPECT_EQ(getValue<double>("errors", json["stream_master"]), 0.0);
  EXPECT_EQ(getValue<double>("messages", json["stream_master"]), 0.0);
  EXPECT_LT(getValue<double>("runtime", json["stream_master"]), 10.0);
  EXPECT_EQ(getValue<std::string>("state", json["stream_master"]),
            "Not Started");
  EXPECT_NEAR(getValue<uint64_t>("timestamp", json), getTimestampMs(), 1000);
}

TEST(StatusWriter, showTimeToNextMessage) {
  StreamMasterInfo sm;
  sm.setTimeToNextMessage(std::chrono::milliseconds{1000});

  FileWriter::Status::StatusWriter Writer;
  Writer.write(sm);
  nlohmann::json json = nlohmann::json::parse(Writer.getJson());

  EXPECT_EQ(getValue<double>("next_message_eta_ms", json), 1000.0);
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

  EXPECT_EQ(getValue<double>("Mbytes", json["stream_master"]),
            MessageSizeBytes * 1e-6);
  EXPECT_EQ(getValue<double>("errors", json["stream_master"]), 0.0);
  EXPECT_EQ(getValue<double>("messages", json["stream_master"]), 1.0);
  EXPECT_EQ(getValue<std::string>("state", json["stream_master"]),
            "Not Started");
  EXPECT_NEAR(getValue<uint64_t>("timestamp", json), getTimestampMs(), 1000);
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

  EXPECT_EQ(getValue<double>("Mbytes", json["stream_master"]), 0.0);
  EXPECT_EQ(getValue<double>("errors", json["stream_master"]), 1.0);
  EXPECT_EQ(getValue<double>("messages", json["stream_master"]), 0.0);
  EXPECT_EQ(getValue<std::string>("state", json["stream_master"]),
            "Not Started");
  EXPECT_NEAR(getValue<uint64_t>("timestamp", json), getTimestampMs(), 1000);
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

  EXPECT_EQ(getValue<double>("average",
                             json["streamer"][Topic]["rates"]["message_size"]),
            0.0);
  EXPECT_EQ(getValue<double>("standard_deviation",
                             json["streamer"][Topic]["rates"]["message_size"]),
            0.0);
  EXPECT_EQ(getValue<double>("Mbytes", json["streamer"][Topic]["rates"]), 0.0);
  EXPECT_EQ(getValue<int>("errors", json["streamer"][Topic]["rates"]), 0);
  EXPECT_EQ(getValue<int>("messages", json["streamer"][Topic]["rates"]), 0);
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

  EXPECT_EQ(getValue<double>("average",
                             json["streamer"][Topic]["rates"]["message_size"]),
            MessageSizeBytes / NumMessages * 1e-6);
  EXPECT_EQ(getValue<double>("standard_deviation",
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
  std::pair<double, double> MessageSize = Message.messageSizeStats();
  EXPECT_DOUBLE_EQ(MessageSize.first, 0.050688);
  EXPECT_DOUBLE_EQ(MessageSize.second, 0.0297077677833032);
}
