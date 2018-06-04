#include "Streamer.h"
#include "json.h"

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
TEST_F(T_Streamer, errorNoSourcesOnEmptyConstructor) {
  Streamer Stream;
  EXPECT_EQ(Stream.numSources(), 0ul);
  EXPECT_TRUE(Stream.runStatus() == StreamerError::NOT_INITIALIZED());
}

TEST_F(T_Streamer, errorIfBrokerArgumentIsEmpty) {
  EXPECT_ANY_THROW(Streamer("", "streamer.test.topic", EmptyOpts));
}

TEST_F(T_Streamer, errorIfTopicArgumentIsEmpty) {
  EXPECT_ANY_THROW(Streamer(BrokerAddress, "", EmptyOpts));
}

TEST_F(T_Streamer, errorIfRdkafkaCanTCreateConfiguration) {
  setConfigurationInvalid();
  Streamer Stream(BrokerAddress, "any.random.topic", EmptyOpts);

  EXPECT_EQ(Stream.numSources(), 0ul);
  EXPECT_TRUE(getStatus(Stream) == StreamerError::CONFIGURATION_ERROR());
}

TEST_F(T_Streamer, rdkafkaCanCreateConfiguration) {
  setConfigurationValid();
  addTopic("any.random.topic");
  Streamer Stream(BrokerAddress, "any.random.topic", EmptyOpts);
  EXPECT_TRUE(getStatus(Stream) == StreamerError::WRITING());
}

TEST_F(T_Streamer, configurationDoesnTAcceptOptionIsNoError) {
  // set configuration valid, option invalid
  setConfigurationValid();
  resetConfigurationOptions();
  setConfigurationOptionInvalid();
  addTopic("any.random.topic");

  // Create StreamerOptions and apply some options from json to it
  FileWriter::StreamerOptions Opts;
  auto Document = nlohmann::json::parse(R""("{
    "ms-before-start": 10,
    "consumer-timeout-ms": 10
  })"");
  Opts.setStreamerOptions(Document);

  Streamer Stream(BrokerAddress, "any.random.topic", Opts);

  // invalid option is not added to the list
  EXPECT_EQ(getConfigurationOptionsSize(), 0ul);
  EXPECT_TRUE(getStatus(Stream) == StreamerError::WRITING());

  resetTopics();
}

TEST_F(T_Streamer, errorIfTopicIsNotPresentInBroker) {
  // make sure there are no topic registered
  resetTopics();
  setMetadataReturnValueOk();

  // remove topic from broker
  resetTopicPartitionMetadata();

  Streamer Stream(BrokerAddress, "missing.topic", EmptyOpts);
  EXPECT_TRUE(getStatus(Stream) == StreamerError::TOPIC_PARTITION_ERROR());
}

TEST_F(T_Streamer, connectionSuccessful) {
  addTopic("test.topic");

  Streamer Stream(BrokerAddress, "test.topic", EmptyOpts);
  EXPECT_TRUE(getStatus(Stream) == StreamerError::WRITING());
}

TEST_F(T_Streamer, consumeTimeoutIsNotError) {
  addTopic("test.topic");
  Streamer Stream(BrokerAddress, "test.topic", EmptyOpts);
  FileWriter::DemuxTopic Demux("streamer.test.topic");
  FileWriter::ProcessMessageResult ProcessResult = Stream.write(Demux);
  EXPECT_TRUE(ProcessResult.is_OK());
}
