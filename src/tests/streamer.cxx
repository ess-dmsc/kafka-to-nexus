#include "Streamer.h"

#include <gtest/gtest.h>
#include <librdkafka/utils.h>

using Streamer = FileWriter::Streamer;
using StreamerError = FileWriter::Status::StreamerError;
const std::string BrokerAddress{"192.168.10.11"};
FileWriter::StreamerOptions EmptyOpts;

class T_Streamer : public ::testing::Test {

protected:
  StreamerError getStatus(Streamer &Stream) {

    if (Stream.IsConnected.valid()) {
      if (Stream.IsConnected.wait_for(std::chrono::milliseconds(1000)) !=
          std::future_status::ready) {
        LOG(Sev::Critical, "... still not ready");
      } else {
        Stream.RunStatus = Stream.IsConnected.get();
      }
      return Stream.RunStatus;
    }
    return StreamerError::UNKNOWN_ERROR();
  }
};

// make sure that a topic exists/not exists
TEST_F(T_Streamer, error_no_sources_on_empty_constructor) {
  Streamer Stream;
  EXPECT_EQ(Stream.numSources(), 0ul);
  EXPECT_TRUE(Stream.runStatus() == StreamerError::NOT_INITIALIZED());
}

TEST_F(T_Streamer, error_if_broker_argument_is_empty) {
  EXPECT_ANY_THROW(Streamer("", "streamer.test.topic", EmptyOpts));
}

TEST_F(T_Streamer, error_if_topic_argument_is_empty) {
  EXPECT_ANY_THROW(Streamer(BrokerAddress, "", EmptyOpts));
}

TEST_F(T_Streamer, error_if_rdkafka_can_t_create_configuration) {
  setConfigurationInvalid();
  Streamer Stream(BrokerAddress, "any.random.topic", EmptyOpts);

  EXPECT_EQ(Stream.numSources(), 0ul);
  EXPECT_TRUE(getStatus(Stream) == StreamerError::CONFIGURATION_ERROR());
}

TEST_F(T_Streamer, rdkafka_can_create_configuration) {
  setConfigurationValid();
  addTopic("any.random.topic");
  Streamer Stream(BrokerAddress, "any.random.topic", EmptyOpts);
  EXPECT_TRUE(getStatus(Stream) == StreamerError::WRITING());
}

TEST_F(T_Streamer, configuration_doesn_t_accept_option_is_no_error) {
  // set configuration valid, option invalid
  setConfigurationValid();
  resetConfigurationOptions();
  setConfigurationOptionInvalid();
  addTopic("any.random.topic");

  // create option
  FileWriter::StreamerOptions Opts;
  rapidjson::Document Document;
  Document.Parse(
      std::string("{ \"ms-before-start\": 10, \"consumer-timeout-ms\" : 10 }")
          .c_str());
  Opts.setStreamerOptions((rapidjson::Value *)&Document);

  Streamer Stream(BrokerAddress, "any.random.topic", Opts);

  // invalid option is not added to the list
  EXPECT_EQ(getConfigurationOptionsSize(), 0ul);
  EXPECT_TRUE(getStatus(Stream) == StreamerError::WRITING());

  resetTopics();
}

TEST_F(T_Streamer, error_if_topic_is_not_present_in_broker) {
  // make sure there are no topic registered
  resetTopics();
  setMetadataReturnValueOk();

  // remove topic from broker
  resetTopicPartitionMetadata();

  Streamer Stream(BrokerAddress, "missing.topic", EmptyOpts);
  EXPECT_TRUE(getStatus(Stream) == StreamerError::TOPIC_PARTITION_ERROR());
}

TEST_F(T_Streamer, connection_successful) {
  addTopic("test.topic");

  Streamer Stream(BrokerAddress, "test.topic", EmptyOpts);
  EXPECT_TRUE(getStatus(Stream) == StreamerError::WRITING());
}

TEST_F(T_Streamer, consume_timeout_is_not_error) {
  addTopic("test.topic");
  Streamer Stream(BrokerAddress, "test.topic", EmptyOpts);
  FileWriter::DemuxTopic Demux("streamer.test.topic");
  FileWriter::ProcessMessageResult ProcessResult = Stream.write(Demux);
  EXPECT_TRUE(ProcessResult.is_OK());
}
