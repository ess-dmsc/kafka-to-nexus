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

namespace FlatbufferReaderRegistry {

std::map<FlatbufferReaderRegistry::Key, FlatbufferReaderRegistry::Value> &
items() {
  static std::map<FlatbufferReaderRegistry::Key,
                  FlatbufferReaderRegistry::Value> _items;
  return _items;
}

FlatbufferReaderRegistry::Value &
find(FlatbufferReaderRegistry::Key const &key) {
  static FlatbufferReaderRegistry::Value empty;
  auto &_items = items();
  auto f = _items.find(key);
  if (f == _items.end()) {
    return empty;
  }
  return f->second;
}

FlatbufferReader::ptr &find(Msg const &msg) {
  static_assert(FLATBUFFERS_LITTLEENDIAN, "Requires currently little endian");
  if (msg.size() < 8) {
    LOG(Sev::Warning, "flatbuffer message is too small: {} expect at least 8",
        msg.size());
    static FlatbufferReader::ptr empty;
    return empty;
  }
  FlatbufferReaderRegistry::Key key;
  memcpy(&key, msg.data() + 4, 4);
  return find(key);
}

void add(FBID fbid, FlatbufferReader::ptr &&item) {
  auto &m = items();
  if (m.find(fbid) != m.end()) {
    auto s =
        fmt::format("ERROR FlatbufferReader for FBID [{:.{}}] exists already",
                    fbid.data(), fbid.size());
    throw std::runtime_error(s);
  }
  m[fbid] = std::move(item);
}
}
}
