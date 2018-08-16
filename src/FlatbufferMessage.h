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
  explicit BufferTooSmallError(const std::string &what)
      : std::runtime_error(what){};
};

class UnknownFlatbufferID : public std::runtime_error {
public:
  explicit UnknownFlatbufferID(const std::string &what)
      : std::runtime_error(what){};
};

class NotValidFlatbuffer : public std::runtime_error {
public:
  explicit NotValidFlatbuffer(const std::string &what)
      : std::runtime_error(what){};
};
/// \todo Profile code to determine if the exceptions are a performance problem
class FlatbufferMessage {
public:
  FlatbufferMessage();
  FlatbufferMessage(char const *Pointer, size_t const Size);
  ~FlatbufferMessage() = default;
  bool isValid() const { return Valid; };
  std::string getSourceName() const { return Sourcename; };
  std::uint64_t getTimestamp() const { return Timestamp; };
  char const *const data() const { return DataPtr; };
  size_t size() const { return DataSize; };

private:
  void extractPacketInfo();
  char const *const DataPtr{nullptr};
  size_t const DataSize{0};
  std::string Sourcename;
  std::uint64_t Timestamp{0};
  bool Valid{false};
};
} // namespace FileWriter
