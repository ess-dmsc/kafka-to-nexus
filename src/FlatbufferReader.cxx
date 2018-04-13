#include "FlatbufferReader.h"
#include <flatbuffers/flatbuffers.h>

namespace FileWriter {

namespace FlatbufferReaderRegistry {

  std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &
getReaders() {
  static std::map<std::string,
                  FlatbufferReaderRegistry::ReaderPtr>
      _items;
  return _items;
}

FlatbufferReaderRegistry::ReaderPtr &
find(std::string const &key) {
  static FlatbufferReaderRegistry::ReaderPtr empty;
  auto &_items = getReaders();
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
  std::string key(msg.data() + 4, 4);
  return find(key);
}

void add(std::string FlatbufferID, FlatbufferReader::ptr &&item) {
  auto &m = getReaders();
  if (FlatbufferID.size() != 4) {
    throw std::runtime_error("FlatbufferReader ID must be a 4 character string.");
  }
  if (m.find(FlatbufferID) != m.end()) {
    auto s =
        fmt::format("ERROR FlatbufferReader for ID [{}] exists already", FlatbufferID);
    throw std::runtime_error(s);
  }
  m[FlatbufferID] = std::move(item);
}
} // namespace FlatbufferReaderRegistry
} // namespace FileWriter
