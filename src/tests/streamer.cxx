#include "Streamer.h"

#include <librdkafka/utils.h>
#include <gtest/gtest.h>

#include <iostream>

using Streamer = FileWriter::Streamer;
using SEC = Streamer::SEC;
const std::string BrokerAddress{ "192.168.10.11" };
FileWriter::StreamerOptions EmptyOpts;

// make sure that a topic exists/not exists

TEST(Streamer, error_no_sources_on_empty_constructor) {
  Streamer Stream;
  EXPECT_EQ(Stream.numSources(), 0ul);
  EXPECT_EQ(Stream.runStatus(), SEC::not_initialized);
}

TEST(Streamer, error_if_broker_argument_is_empty) {
  Streamer Stream("", "streamer.test.topic", EmptyOpts);
  EXPECT_EQ(Stream.numSources(), 0ul);
  EXPECT_EQ(Stream.runStatus(), SEC::not_initialized);
}

TEST(Streamer, error_if_topic_argument_is_empty) {
  Streamer Stream(BrokerAddress, "", EmptyOpts);
  EXPECT_EQ(Stream.numSources(), 0ul);
  EXPECT_EQ(Stream.runStatus(), SEC::not_initialized);
}

TEST(Streamer, error_if_rdkafka_can_t_create_configuration) {
  setConfigurationInvalid();
  Streamer Stream(BrokerAddress, "any.random.topic", EmptyOpts);
  EXPECT_EQ(Stream.numSources(), 0ul);
  EXPECT_EQ(Stream.runStatus(), SEC::configuration_error);
}

TEST(Streamer, rdkafka_can_create_configuration) {
  setConfigurationValid();
  Streamer Stream(BrokerAddress, "any.random.topic", EmptyOpts);
  EXPECT_NE(Stream.runStatus(), SEC::configuration_error);
}

TEST(Streamer, configuration_doesn_t_accept_option_is_no_error) {
  // set configuration valid, option invalid
  setConfigurationValid();
  resetConfigurationOptions();
  setConfigurationOptionInvalid();
  addTopic("any.random.topic");

  // create option
  FileWriter::StreamerOptions Opts;
  rapidjson::Document Document;
  Document.Parse(std::string(
      "{ \"ms-before-start\": 10, \"consumer-timeout-ms\" : 10 }").c_str());
  Opts.setStreamerOptions((rapidjson::Value *)&Document);

  Streamer Stream(BrokerAddress, "any.random.topic", Opts);
  // invalid option is not added to the list
  EXPECT_EQ(getConfigurationOptionsSize(), 0ul);

  // the final state will be "writing". More in general <0 represent error,> 0
  // success
  EXPECT_GE(int(Stream.runStatus()), 0);

  resetTopics();
}

TEST(Streamer, configuration_accepts_options) {
  setConfigurationValid();
  setConfigurationOptionValid();
  resetConfigurationOptions();

  addTopic("any.random.topic");

  FileWriter::StreamerOptions Opts;
  rapidjson::Document Document;
  Document.Parse(std::string(
      "{ \"ms-before-start\": 10, \"consumer-timeout-ms\" : 10 }").c_str());
  Opts.setStreamerOptions((rapidjson::Value *)&Document);

  Streamer Stream(BrokerAddress, "any.random.topic", Opts);
  // valid options are not added to the list. the size must be 2 + number of
  // default options
  EXPECT_GT(getConfigurationOptionsSize(), 2ul);

  resetTopics();
}

TEST(Streamer, error_if_topic_is_not_present_in_broker) {
  // make sure there are no topic registered
  resetTopics();
  setMetadataReturnValueOk();

  // remove topic from broker
  resetTopicPartitionMetadata();

  Streamer Stream(BrokerAddress, "missing.topic", EmptyOpts);
  std::cout << Err2Str(Stream.runStatus()) << "\n";
  EXPECT_EQ(Stream.runStatus(), SEC::topic_partition_error);
}

TEST(Streamer, connection_successful) {
  addTopic("test.topic");

  Streamer Stream(BrokerAddress, "test.topic", EmptyOpts);
  EXPECT_EQ(Stream.runStatus(), SEC::writing);
}

TEST(Streamer, consume_timeout_is_not_error) {
  Streamer Stream(BrokerAddress, "test.topic", EmptyOpts);
  FileWriter::DemuxTopic Demux("streamer.test.topic");
  FileWriter::ProcessMessageResult ProcessResult = Stream.write(Demux);
  EXPECT_TRUE(ProcessResult.is_OK());
}
