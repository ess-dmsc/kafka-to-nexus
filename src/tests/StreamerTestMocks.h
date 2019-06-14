#pragma once

#include <trompeloeil.hpp>

#include "Streamer.h"

namespace FileWriter {

class ConsumerEmptyStandIn
    : public trompeloeil::mock_interface<KafkaW::ConsumerInterface> {
public:
  explicit ConsumerEmptyStandIn(const KafkaW::BrokerSettings &Settings){};
  IMPLEMENT_MOCK1(addTopic);
  IMPLEMENT_MOCK2(addTopicAtTimestamp);
  IMPLEMENT_MOCK1(topicPresent);
  IMPLEMENT_MOCK1(queryTopicPartitions);
  IMPLEMENT_MOCK0(poll);
  IMPLEMENT_MOCK2(offsetsForTimesAllPartitions);
};

class StreamerStandIn : public Streamer {
public:
  StreamerStandIn()
      : Streamer("SomeBroker", "SomeTopic", StreamerOptions(),
                 std::make_unique<ConsumerEmptyStandIn>(
                     StreamerOptions().BrokerSettings)) {}
  explicit StreamerStandIn(StreamerOptions Opts)
      : Streamer("SomeBroker", "SomeTopic", std::move(Opts),
                 std::make_unique<ConsumerEmptyStandIn>(
                     StreamerOptions().BrokerSettings)) {}
  using Streamer::ConsumerInitialised;
  using Streamer::Options;
};

class DemuxerStandIn : public DemuxTopic {
public:
  explicit DemuxerStandIn(std::string Topic) : DemuxTopic(std::move(Topic)) {}
  MAKE_MOCK1(process_message, ProcessMessageResult(FlatbufferMessage const &),
             override);
};
}

class StreamerNoTimestampTestDummyReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override {
    return true;
  }
  std::string
  source_name(FileWriter::FlatbufferMessage const &Message) const override {
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t
  timestamp(FileWriter::FlatbufferMessage const &Message) const override {
    return 0;
  }
};

class StreamerTestDummyReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override {
    return true;
  }
  std::string
  source_name(FileWriter::FlatbufferMessage const &Message) const override {
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t
  timestamp(FileWriter::FlatbufferMessage const &Message) const override {
    return 1;
  }
};

class StreamerHighTimestampTestDummyReader
    : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override {
    return true;
  }
  std::string
  source_name(FileWriter::FlatbufferMessage const &Message) const override {
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t
  timestamp(FileWriter::FlatbufferMessage const &Message) const override {
    return 5000000;
  }
};

class StreamerMessageSlightlyAfterStopTestDummyReader
    : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override {
    return true;
  }
  std::string
  source_name(FileWriter::FlatbufferMessage const &Message) const override {
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t
  timestamp(FileWriter::FlatbufferMessage const &Message) const override {
    return 1;
  }
};

class WriterModuleStandIn : public FileWriter::HDFWriterModule {
public:
  MAKE_MOCK2(parse_config, void(std::string const &, std::string const &),
             override);
  MAKE_MOCK2(init_hdf, InitResult(hdf5::node::Group &, std::string const &),
             override);
  MAKE_MOCK1(reopen, InitResult(hdf5::node::Group &), override);
  MAKE_MOCK1(write, void(FileWriter::FlatbufferMessage const &), override);
  MAKE_MOCK0(flush, int32_t(), override);
  MAKE_MOCK0(close, int32_t(), override);
};
