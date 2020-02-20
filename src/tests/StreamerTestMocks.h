#pragma once

#include "FlatbufferMessage.h"
#include "FlatbufferReader.h"
#include "KafkaW/Consumer.h"
#include "WriterModuleBase.h"
#include <trompeloeil.hpp>

namespace FileWriter {

class ConsumerEmptyStandIn
    : public trompeloeil::mock_interface<KafkaW::ConsumerInterface> {
public:
  explicit ConsumerEmptyStandIn(const KafkaW::BrokerSettings &Settings){
      UNUSED_ARG(Settings)};
  IMPLEMENT_MOCK1(addTopic);
  IMPLEMENT_MOCK2(addTopicAtTimestamp);
  IMPLEMENT_MOCK3(addPartitionAtOffset);
  IMPLEMENT_MOCK1(topicPresent);
  IMPLEMENT_MOCK1(queryTopicPartitions);
  IMPLEMENT_MOCK0(poll);
  IMPLEMENT_MOCK2(offsetsForTimesAllPartitions);
  IMPLEMENT_MOCK2(getHighWatermarkOffset);
  IMPLEMENT_MOCK1(getCurrentOffsets);
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

class StreamerMessageFailsValidationTestDummyReader
    : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override {
    UNUSED_ARG(Message)
    return false;
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

class WriterModuleStandIn : public WriterModule::Base {
public:
  MAKE_MOCK1(parse_config, void(std::string const &), override);
  MAKE_MOCK2(init_hdf,
             WriterModule::InitResult(hdf5::node::Group &, std::string const &),
             override);
  MAKE_MOCK1(reopen, WriterModule::InitResult(hdf5::node::Group &), override);
  MAKE_MOCK1(write, void(FileWriter::FlatbufferMessage const &), override);
};
