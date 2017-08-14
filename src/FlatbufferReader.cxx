#include "FlatbufferReader.h"

namespace FileWriter {

/// `x` must be a string of exactly 4 characters. `NULL` terminator is not
/// required.
FBID fbid_from_str(char const *x) {
  FBID ret;
  memcpy(ret.data(), x, 4);
  return ret;
}

std::map<FBID, FlatbufferReader::ptr> &FlatbufferReaderRegistry::items() {
  static std::map<FBID, FlatbufferReader::ptr> _items;
  return _items;
}

FlatbufferReader::ptr &FlatbufferReaderRegistry::find(FBID const &fbid) {
  static FlatbufferReader::ptr empty;
  auto &_items = items();
  auto f = _items.find(fbid);
  if (f == _items.end()) {
    return empty;
  }
  return f->second;
}
}
