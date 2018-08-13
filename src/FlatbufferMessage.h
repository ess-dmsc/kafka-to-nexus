#pragma once

#include "KafkaW/Msg.h"
#include "logger.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <vector>

namespace FileWriter {
  class BufferTooSmallError : public std::runtime_error {
  public:
    BufferTooSmallError(const std::string &what) : std::runtime_error(what){};
  };
  
  class UnknownFlatbufferID : public std::runtime_error {
  public:
    UnknownFlatbufferID(const std::string &what) : std::runtime_error(what){};
  };
  
  class FlatbufferMessage {
  public:
    FlatbufferMessage();
    FlatbufferMessage(char const *Pointer, size_t const Size);
    ~FlatbufferMessage();
    bool isValid() const {return Valid;};
    std::string getSourceName() const {return Sourcename;};
    std::uint64_t getTimestamp() const {return Timestamp;};
    char* const data() const {return DataPtr;};
    size_t size() const {return DataSize;};
  private:
    void extractPacketInfo();
    char* const DataPtr{nullptr};
    size_t DataSize{0};
    std::string Sourcename;
    std::uint64_t Timestamp{0};
    bool Valid{false};
  };
} // namespace FileWriter
