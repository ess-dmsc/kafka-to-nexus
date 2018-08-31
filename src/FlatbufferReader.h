#pragma once

#include "logger.h"
#include <array>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace FileWriter {

class FlatbufferMessage;

/// Interface for reading essential information from the flatbuffer which is
/// needed for example to extract timing information and name of the source.
/// Example: Please see `src/schemas/ev42/ev42_rw.cpp`.

class FlatbufferReader {
public:
  using ptr = std::unique_ptr<FlatbufferReader>;
  /// Run the flatbuffer verification and return the result.
  virtual bool verify(FlatbufferMessage const &Message) const = 0;
  /// Extract the 'source_name' from the flatbuffer message.
  virtual std::string source_name(FlatbufferMessage const &Message) const = 0;
  /// Extract the timestamp.
  virtual uint64_t timestamp(FlatbufferMessage const &Message) const = 0;
};
} // namespace FileWriter
