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

TEST(StatusWriter, emptyWriterHasDefaultFields) {
  NLJSONWriter Writer;
  StreamMasterInfo sm;
  nlohmann::json json = Writer.get();
  EXPECT_EQ(getStringValue("type", json), "stream_master_status");
  EXPECT_EQ(getIntegerValue("next_message_eta_ms", json), 0);
  EXPECT_EQ(getIntegerValue("job_id", json), 0);
}

TEST(StatusWriter, addEmptyStreamMasterInfoUsesDefaults) {
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

TEST(StatusWriter, showTimeToNextMessage) {
  StreamMasterInfo sm;
  sm.setTimeToNextMessage(std::chrono::milliseconds{1000});

  FileWriter::Status::NLJSONWriter Writer;
  Writer.write(sm);
  nlohmann::json json = Writer.get();

  EXPECT_EQ(getDoubleValue("next_message_eta_ms", json), 1000.0);
}

TEST(StatusWriter, addMessageUpdatesStreamMaster) {
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

TEST(StatusWriter, addErrorUpdatesStreamMaster) {
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

TEST(StatusWriter, addEmptyMessageInfo) {
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
  EXPECT_EQ(getDoubleValue("frequency", json["streamer"][Topic]["statistics"]),
            0.0);
  EXPECT_EQ(getDoubleValue("throughput", json["streamer"][Topic]["statistics"]),
            0.0);
}

TEST(StatusWriter, addValidMessageUpdatesStreamerInfo) {
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
      getDoubleValue("frequency", json["streamer"][Topic]["statistics"]),
      NumMessages /
          std::chrono::duration_cast<std::chrono::seconds>(SinceLastMessage)
              .count());
  EXPECT_EQ(
      getDoubleValue("throughput", json["streamer"][Topic]["statistics"]),
      MessageSizeBytes * 1e-6 /
          std::chrono::duration_cast<std::chrono::seconds>(SinceLastMessage)
              .count());
}

TEST(StatFunctions, messageSize) {
  const size_t NumMessages = 100;
  const size_t MessageBytes = 1024;
  MessageInfo Message;
  for (size_t i = 0; i < NumMessages; ++i) {
    Message.message(MessageBytes * i);
  }
  std::pair<double, double> MessageSize =
      FileWriter::Status::messageSize(Message);
  EXPECT_DOUBLE_EQ(MessageSize.first, 0.050688);
  EXPECT_DOUBLE_EQ(MessageSize.second, 0.0297077677833032);
}

TEST(StatFunctions, messageFrequency) {
  std::chrono::milliseconds TimeBetweenMessages{1500};
  const size_t NumMessages = 100;
  const size_t MessageBytes = 1024;
  MessageInfo Message;
  for (size_t i = 0; i < NumMessages; ++i) {
    Message.message(MessageBytes * i);
  }

  double Frequency =
      FileWriter::Status::messageFrequency(Message, TimeBetweenMessages);
  EXPECT_DOUBLE_EQ(Frequency,
                   NumMessages / (1e-3 * TimeBetweenMessages.count()));
}

TEST(StatFunctions, messageThroughput) {
  std::chrono::milliseconds TimeBetweenMessages{1500};
  const size_t NumMessages = 100;
  const size_t MessageBytes = 1024;
  MessageInfo Message;
  for (size_t i = 0; i < NumMessages; ++i) {
    Message.message(MessageBytes * i);
  }

  double Throughput =
      FileWriter::Status::messageThroughput(Message, TimeBetweenMessages);
  EXPECT_DOUBLE_EQ(Throughput,
                   ((NumMessages - 1) * NumMessages / 2 * MessageBytes * 1e-6) /
                       (1e-3 * TimeBetweenMessages.count()));
}
