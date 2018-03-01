#include "Streamer.h"
//#include "tests/mock_rdkafkacpp_generated.h"
#include <librdkafka/rdkafkacpp.h>

#include "/home/scratch/librdkafka-fake/build/include/utils.h"
#include <gtest/gtest.h>

#include <iostream>

using Streamer = FileWriter::Streamer;
using SEC = Streamer::SEC;
const std::string BrokerAddress{ "192.168.10.11" };
FileWriter::StreamerOptions EmptyOpts;

// make sure that a topic exists/not exists

TEST(Streamer, error_no_sources_on_empty_constructor) {
  Streamer Stream;
  EXPECT_EQ(Stream.numSources(), 0);
  EXPECT_EQ(Stream.runStatus(), SEC::not_initialized);
}

TEST(Streamer, error_ifbroker_argument_is_empty) {
  Streamer Stream("", "streamer.test.topic", EmptyOpts);
  EXPECT_EQ(Stream.numSources(), 0);
  EXPECT_EQ(Stream.runStatus(), SEC::not_initialized);
}

TEST(Streamer, error_if_topic_argument_is_empty) {
  Streamer Stream(BrokerAddress, "", EmptyOpts);
  EXPECT_EQ(Stream.numSources(), 0);
  EXPECT_EQ(Stream.runStatus(), SEC::not_initialized);
}

TEST(Streamer, error_if_rdkafka_can_t_create_configuration) {
  setConfigurationInvalid();
  Streamer Stream(BrokerAddress, "any.random.topic", EmptyOpts);
  EXPECT_EQ(Stream.numSources(), 0);
  EXPECT_EQ(Stream.runStatus(), SEC::configuration_error);
}

TEST(Streamer, rdkafka_can_create_configuration) {
  setConfigurationValid();
  Streamer Stream(BrokerAddress, "any.random.topic", EmptyOpts);
  EXPECT_NE(Stream.runStatus(), SEC::configuration_error);
}

TEST(Streamer, configuration_doesn_t_accept_options) {
  // set configuration valid, option invalid
  setConfigurationValid();
  resetConfigurationOptions();
  setConfigurationOptionInvalid();
  addTopicPartitionMetadata("any.random.topic", { 0 });
  // create option
  FileWriter::StreamerOptions Opts;
  rapidjson::Document Document;
  Document.Parse(std::string(
      "{ \"ms-before-start\": 10, \"consumer-timeout-ms\" : 10 }").c_str());
  Opts.setStreamerOptions((rapidjson::Value *)&Document);

  Streamer Stream(BrokerAddress, "any.random.topic", Opts);
  // invalid option is not added to the list
  EXPECT_EQ(getConfigurationOptionsSize(), 0ul);

  // the final state will be "writing". More in general <0 represent error, >0
  // success
  EXPECT_GE(int(Stream.runStatus()), 0);
  std::cout << Err2Str(Stream.runStatus()) << std::endl;
}

TEST(Streamer, configuration_accepts_options) {
  setConfigurationValid();
  setConfigurationOptionValid();
  resetConfigurationOptions();

  resetTopicPartitionMetadata();
  addTopicPartitionMetadata("any.random.topic", { 0 });

  FileWriter::StreamerOptions Opts;
  rapidjson::Document Document;
  Document.Parse(std::string(
      "{ \"ms-before-start\": 10, \"consumer-timeout-ms\" : 10 }").c_str());
  Opts.setStreamerOptions((rapidjson::Value *)&Document);

  Streamer Stream(BrokerAddress, "any.random.topic", Opts);
  // valid options are not added to the list. the size must be 2 + number of
  // default options
  EXPECT_GT(getConfigurationOptionsSize(), 2ul);
}

TEST(Streamer, error_if_topic_is_not_present_in_broker) {
  setMetadataReturnValueOk();

  // remove topic from broker
  resetTopicPartitionMetadata();

  Streamer Stream(BrokerAddress, "missing.topic", EmptyOpts);
  EXPECT_EQ(Stream.runStatus(), SEC::topic_partition_error);
}

TEST(Streamer, connection_successful) {
  addTopicPartitionMetadata("test.topic", { 0 });

  Streamer Stream(BrokerAddress, "test.topic", EmptyOpts);
  EXPECT_EQ(Stream.runStatus(), SEC::writing);
}

TEST(Streamer, consume_timeout_is_ok) {
  Streamer Stream(BrokerAddress, "test.topic", EmptyOpts);
  FileWriter::DemuxTopic Demux("streamer.test.topic");
  FileWriter::ProcessMessageResult ProcessResult = Stream.write(Demux);
  EXPECT_TRUE(ProcessResult.is_OK());
}
