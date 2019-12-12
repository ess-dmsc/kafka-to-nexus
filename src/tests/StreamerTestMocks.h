#pragma once

#include <trompeloeil.hpp>

#include "Streamer.h"

namespace FileWriter {

class ConsumerEmptyStandIn
    : public trompeloeil::mock_interface<KafkaW::ConsumerInterface> {
public:
  explicit ConsumerEmptyStandIn(const KafkaW::BrokerSettings &Settings){
      UNUSED_ARG(Settings)};
  IMPLEMENT_MOCK1(addTopic);
  IMPLEMENT_MOCK2(addTopicAtTimestamp);
  IMPLEMENT_MOCK1(topicPresent);
  IMPLEMENT_MOCK1(queryTopicPartitions);
  IMPLEMENT_MOCK0(poll);
  IMPLEMENT_MOCK2(offsetsForTimesAllPartitions);
  IMPLEMENT_MOCK2(getHighWatermarkOffset);
  IMPLEMENT_MOCK1(getCurrentOffsets);
};

class DemuxerStandIn : public DemuxTopic {
public:
  explicit DemuxerStandIn(std::string Topic) : DemuxTopic(std::move(Topic)) {}
  ProcessMessageResult process_message(FlatbufferMessage const &) override {
    return ProcessMessageResult::OK;
  }
};

class StreamerStandIn : public Streamer {
public:
  StreamerStandIn()
      : Streamer("SomeBroker", "SomeTopic", StreamerOptions(),
                 std::make_unique<ConsumerEmptyStandIn>(
                     StreamerOptions().BrokerSettings),
                 std::make_shared<DemuxerStandIn>("SomeTopic")) {}
  explicit StreamerStandIn(StreamerOptions Opts)
      : Streamer("SomeBroker", "SomeTopic", std::move(Opts),
                 std::make_unique<ConsumerEmptyStandIn>(
                     StreamerOptions().BrokerSettings),
                 std::make_shared<DemuxerStandIn>("SomeTopic")) {}
  StreamerStandIn(StreamerOptions Opts, std::shared_ptr<DemuxTopic> &Demuxer)
      : Streamer("SomeBroker", "SomeTopic", std::move(Opts),
                 std::make_unique<ConsumerEmptyStandIn>(
                     StreamerOptions().BrokerSettings),
                 Demuxer) {}
  using Streamer::ConsumerInitialised;
  using Streamer::Options;
};

} // namespace FileWriter

class StreamerNoTimestampTestDummyReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return true;
  }
  std::string
  source_name(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t
  timestamp(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return 0;
  }
};

class StreamerTestDummyReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return true;
  }
  std::string
  source_name(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t
  timestamp(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return 1;
  }
};

class StreamerHighTimestampTestDummyReader
    : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return true;
  }
  std::string
  source_name(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t
  timestamp(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return 5000000;
  }
};

class StreamerMessageSlightlyAfterStopTestDummyReader
    : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return true;
  }
  std::string
  source_name(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return std::string("SomeRandomSourceName");
  }
  std::uint64_t
  timestamp(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return 1;
  }
};

class WriterModuleStandIn : public FileWriter::HDFWriterModule {
public:
  MAKE_MOCK1(parse_config, void(std::string const &), override);
  MAKE_MOCK2(init_hdf, InitResult(hdf5::node::Group &, std::string const &),
             override);
  MAKE_MOCK1(reopen, InitResult(hdf5::node::Group &), override);
  MAKE_MOCK1(write, void(FileWriter::FlatbufferMessage const &), override);
};
