#include "FlatbufferReader.h"
#include <flatbuffers/flatbuffers.h>

namespace FileWriter {

/// `x` must be a string of exactly 4 characters. `NULL` terminator is not
/// required.
FBID fbid_from_str(char const *x) {
  FBID ret;
  memcpy(ret.data(), x, 4);
  return ret;
}

std::map<FlatbufferReaderRegistry::K, FlatbufferReaderRegistry::V> &
FlatbufferReaderRegistry::items() {
  static std::map<FlatbufferReaderRegistry::K, FlatbufferReaderRegistry::V>
      _items;
  return _items;
}

FlatbufferReaderRegistry::V &
FlatbufferReaderRegistry::find(FlatbufferReaderRegistry::K const &key) {
  static FlatbufferReaderRegistry::V empty;
  auto &_items = items();
  auto f = _items.find(key);
  if (f == _items.end()) {
    return empty;
  }
  return f->second;
}

FlatbufferReader::ptr &FlatbufferReaderRegistry::find(Msg const &msg) {
  static_assert(FLATBUFFERS_LITTLEENDIAN, "Requires currently little endian");
  if (msg.size < 8) {
    LOG(4, "flatbuffer message is too small: {} expect at least 8", msg.size);
    static FlatbufferReader::ptr empty;
    return empty;
  }
  FlatbufferReaderRegistry::K key;
  memcpy(&key, msg.data + 4, 4);
  return find(key);
}
}
