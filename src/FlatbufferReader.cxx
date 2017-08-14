#include "FlatbufferReader.h"

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
}
