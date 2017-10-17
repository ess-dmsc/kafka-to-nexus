#include <algorithm>
#include <cstdint>
#include <gtest/gtest.h>
#include <random>
#include <stdexcept>

#include <StatusWriter.hpp>
#include <StreamMaster.hpp>
#include <Streamer.hpp>
#include <librdkafka/rdkafkacpp.h>

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

static std::mt19937_64 rng;

std::ostream &operator<<(std::ostream &os,
                         rapidjson::Document const &document) {
  using namespace rapidjson;
  StringBuffer buffer;
  PrettyWriter<StringBuffer> wr(buffer);
  document.Accept(wr);
  std::cout << buffer.GetString() << "\n";
  return os;
}

namespace FileWriter {
class MockSource {
public:
  MockSource(std::string topic, std::string source)
      : _topic(topic), _source(source){};
  //  MockSource(MockSource &&);
  std::string const &topic() const { return _topic; }
  std::string const &source() const { return _source; }
  bool stop_time(const ESSTimeStamp) { return false; }
  bool start_time(const ESSTimeStamp) { return false; }

  uint32_t processed_messages_count() const {
    return _processed_messages_count;
  }
  ProcessMessageResult process_message(char *msg_data, int msg_size) {
    _processed_messages_count++;
    // int64_t value = rng();
    return ProcessMessageResult::OK();
  }

private:
  std::string _topic;
  std::string _source;
  int _processed_messages_count = 0;
};

class MockDemuxTopic : public MessageProcessor {
public:
  MockDemuxTopic(std::string topic) : _topic(topic){};
  std::string const &topic() const { return _topic; };
  bool stop_time(const ESSTimeStamp) { return false; }
  ProcessMessageResult process_message(char *msg_data, int msg_size) {
    std::string s(msg_data);
    int source_index = 0;
    for (; source_index < (int64_t)_sources.size(); ++source_index) {
      if ((s.find(_sources[source_index].source()) != std::string::npos))
        break;
    }
    return _sources[source_index].process_message(msg_data, msg_size);
  };

  void add_source(std::string s) {
    for (auto item = _sources.begin(); item != _sources.end(); ++item) {
      if ((*item).source() == s)
        return;
    }
    _sources.push_back(MockSource(topic(), s));
  }
  std::vector<MockSource> &sources() { return _sources; };

private:
  std::string _topic;
  std::vector<MockSource> _sources;
  std::vector<int> _message_counter;
};

template <>
FileWriter::ProcessMessageResult
FileWriter::Streamer::write(MockDemuxTopic &mp) {

  return ProcessMessageResult::ERR();
}

} // namespace FileWriter

using namespace FileWriter;

std::string broker{"localhost"};
std::vector<std::string> no_topic = {""};
std::vector<std::string> topic = {"area_detector", "tof_detector", "motor1",
                                  "motor2", "temp"};

// #if 1
std::vector<MockDemuxTopic> no_demux = {MockDemuxTopic("")};
std::vector<MockDemuxTopic> one_demux = {
    MockDemuxTopic("topic.with.multiple.sources")};
std::vector<MockDemuxTopic> demux = {
    MockDemuxTopic("area_detector"), MockDemuxTopic("tof_detector"),
    MockDemuxTopic("motor1"), MockDemuxTopic("motor2"), MockDemuxTopic("temp")};

using MockStreamMaster = StreamMaster<FileWriter::Streamer, MockDemuxTopic>;

TEST(StreamMaster, streamer_report_failure_if_missing_broker_or_topic) {
  {
    MockStreamMaster sm("", one_demux);
    std::this_thread::sleep_for(milliseconds(10));
    EXPECT_EQ(sm.status().value(),
              FileWriter::Status::StreamMasterErrorCode::streamer_error);
  }
  {
    MockStreamMaster sm(broker, no_demux);
    std::this_thread::sleep_for(milliseconds(10));
    EXPECT_EQ(sm.status().value(),
              FileWriter::Status::StreamMasterErrorCode::streamer_error);
  }
}

TEST(StreamMaster, streamer_report_failure_if_wrong_broker) {
  MockStreamMaster sm("no_host", one_demux);
  std::this_thread::sleep_for(milliseconds(10));
  EXPECT_EQ(sm.status().value(),
            FileWriter::Status::StreamMasterErrorCode::streamer_error);
}

TEST(StreamMaster, streamer_report_success_if_broker_and_topic_correct) {
  if (broker == "localhost") {
    std::cerr << "\nWarning: broker not defined, using localhost\n\n";
  }
  MockStreamMaster sm(broker, one_demux);
  std::this_thread::sleep_for(milliseconds(10));
  EXPECT_EQ(sm.status().value(),
            FileWriter::Status::StreamMasterErrorCode::not_started);
}

TEST(StreamMaster, success_setting_start_time) {
  one_demux[0].add_source("for_example_motor01");
  one_demux[0].add_source("for_example_temperature02");
  if (broker == "localhost") {
    std::cerr << "\nWarning: broker not defined, using localhost\n\n";
  }
  MockStreamMaster sm(broker, one_demux);
  EXPECT_TRUE(sm.start_time(ESSTimeStamp(10)));
}

// int main(int argc, char **argv) {
//   rng.seed(1234);
//   ::testing::InitGoogleTest(&argc, argv);
//   for (int i = 1; i < argc; ++i) {
//     std::string opt(argv[i]);
//     size_t found = opt.find("=");
//     if (opt.substr(0, found) == "--kafka_broker")
//       broker = opt.substr(found + 1);
//     if (opt.substr(0, found) == "--help") {
//       std::cout << "\nOptions: "
//                 << "\n"
//                 << "\t--kafka_broker=<host>:<port>[default = 9092]\n";
//     }
//   }

//   return RUN_ALL_TESTS();
// }

// #else
// int main(int argc, char **argv) {}
// #endif
