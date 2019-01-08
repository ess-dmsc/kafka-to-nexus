#pragma once

#include "FlatbufferMessage.h"
#include "logger.h"
#include <array>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace FileWriter {

/// \brief Interface for reading essential information from the flatbuffer which
/// is needed for example to extract timing information and name of the source.
///
/// Example: Please see `src/schemas/ev42/ev42_rw.cpp`.
class FlatbufferReader {
public:
  virtual ~FlatbufferReader() = default;
  using ptr = std::unique_ptr<FlatbufferReader>;

  /// Run the flatbuffer verification and return the result.
  virtual bool verify(FlatbufferMessage const &Message) const = 0;

  /// Extract the 'source_name' from the flatbuffer message.
  virtual std::string source_name(FlatbufferMessage const &Message) const = 0;

  /// Extract the timestamp.
  virtual uint64_t timestamp(FlatbufferMessage const &Message) const = 0;
};

/// \brief Keeps track of the registered FlatbufferReader instances.
///
/// See for example `src/schemas/ev42/ev42_rw.cpp` and search for
/// FlatbufferReaderRegistry.
namespace FlatbufferReaderRegistry {
using ReaderPtr = FlatbufferReader::ptr;
std::map<std::string, ReaderPtr> &getReaders();

FlatbufferReader::ptr &find(std::string const &Key);

void addReader(std::string FlatbufferID, FlatbufferReader::ptr &&item);

template <typename T> class Registrar {
public:
  explicit Registrar(std::string FlatbufferID) {
    FlatbufferReaderRegistry::addReader(FlatbufferID,
                                        std::unique_ptr<T>(new T));
  }
};
} // namespace FlatbufferReaderRegistry
} // namespace FileWriter
