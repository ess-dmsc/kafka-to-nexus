#pragma once

#include "FlatbufferReader.h"
#include "KafkaW/Msg.h"
#include "logger.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <map>
#include <memory>
#include <vector>

namespace FileWriter {

class FlatbufferReader;

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

/// A wrapper around a databuffer which holds a flatbuffer. Used to simplify
/// passing around flatbuffers and the most important pieces of meta-data.
/// \note Does not take ownership of any pointers. You must make sure to
/// free the pointers you passed yourself.
class FlatbufferMessage {
public:
  /// Constructor is used in unit testing code to simplify set-up.
  FlatbufferMessage() = default;
  /// Verifies the data in the flatbuffer to make sure if it is valid.
  /// \param[in] BufferPtr Pointer to memory location containing flatbuffer.
  /// \param[in] Size Size of flatbuffer in bytes.
  /// \throws FileWriter::BufferTooSmallError If the size argument is < 8, it
  /// can not be a flatbuffer as the identifier is stored in bytes 4 to 8.
  /// \throws FileWriter::UnknownFlatbufferID If the flatbuffer ID (bytes 4 to
  /// 8) is not found in the database, this exception is thrown.
  /// \throws FileWriter::NotValidFlatbuffer The constructor runs
  /// FileWriter::FlatbufferReader::verify() on the flatbuffer and this
  /// exception is thrown if it returns false.
  FlatbufferMessage(
      char const *BufferPtr, size_t const Size,
      std::map<std::string, std::unique_ptr<FlatbufferReader>> const &Readers);
  /// Default destructor.
  ~FlatbufferMessage() = default;
  /// Returns the state of the FlatbufferMessage.
  /// \return `true` if valid, `false` if not.
  bool isValid() const { return Valid; };

  /// Get the source name of the flatbuffer. Extracted using
  /// FileWriter::FlatbufferReader::source_name().
  /// \return The source name if flatbuffer is valid, an empty string if it is
  /// not.
  std::string getSourceName() const { return Sourcename; };
  /// Get the timestamp of the flatbuffer. Extracted using
  /// FileWriter::FlatbufferReader::timestamp().
  /// \return The timestamp if flatbuffer is valid, 0 if it is not.
  std::uint64_t getTimestamp() const { return Timestamp; };

  /// Get pointer to flatbuffer.
  /// \return Pointer to flatbuffer data if flatbuffer is valid, `nullptr` if it
  /// is not.
  char const *const data() const { return DataPtr; };

  /// Get size of flatbuffer.
  /// \return Size of flatbuffer in bytes if flatbuffer is valid, 0 if it is
  /// not.
  size_t size() const { return DataSize; };

private:
  void extractPacketInfo(
      std::map<std::string, std::unique_ptr<FlatbufferReader>> const &Readers);
  char const *const DataPtr{nullptr};
  size_t const DataSize{0};
  std::string Sourcename;
  std::uint64_t Timestamp{0};
  bool Valid{false};
};
} // namespace FileWriter
