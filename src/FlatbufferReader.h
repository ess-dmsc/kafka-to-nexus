#pragma once

#include "Msg.h"
#include "logger.h"
#include <array>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace FileWriter {

/// Interface for reading essential information from the flatbuffer which is
/// needed for example to extract timing information and name of the source.
/// Example: Please see `src/schemas/ev42/ev42_rw.cxx`.

class FlatbufferReader {
public:
  using ptr = std::unique_ptr<FlatbufferReader>;
  /// Run the flatbuffer verification and return the result.
  virtual bool verify(Msg const &msg) const = 0;
  /// Extract the 'sourcename' from the flatbuffer message.
  virtual std::string sourcename(Msg const &msg) const = 0;
  /// Extract the timestamp.
  virtual uint64_t timestamp(Msg const &msg) const = 0;
};

using FBID = std::array<char, 4>;
FBID fbid_from_str(char const *x);

/// \brief Keeps track of the registered FlatbufferReader instances.

/// See for example `src/schemas/ev42/ev42_rw.cxx` and search for
/// FlatbufferReaderRegistry.

namespace FlatbufferReaderRegistry {
using Key = FBID;
using Value = FlatbufferReader::ptr;
std::map<Key, Value> &items();
FlatbufferReader::ptr &find(FBID const &fbid);
FlatbufferReader::ptr &find(Msg const &msg);

void add(FBID fbid, FlatbufferReader::ptr &&item);

template <typename T> class Registrar {
public:
  explicit Registrar(FBID fbid) {
    FlatbufferReaderRegistry::add(fbid, std::unique_ptr<T>(new T));
  }
};
}
}
