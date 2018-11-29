#include "FlatbufferReader.h"
#include <flatbuffers/flatbuffers.h>
#include <stdexcept>

namespace FileWriter {

namespace FlatbufferReaderRegistry {

std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &getReaders() {
  static std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> _items;
  return _items;
}

FlatbufferReaderRegistry::ReaderPtr &find(std::string const &key) {
  auto &_items = getReaders();
  try {
    return _items.at(key);
  } catch (std::out_of_range &E) {
    auto s = fmt::format("No such Reader in registry: \"{}\"", E.what());
    std::throw_with_nested(std::out_of_range(s));
  }
}

void addReader(std::string FlatbufferID, FlatbufferReader::ptr &&item) {
  auto &m = getReaders();
  if (FlatbufferID.size() != 4) {
    throw std::runtime_error(
        "FlatbufferReader ID must be a 4 character string.");
  }
  if (m.find(FlatbufferID) != m.end()) {
    auto s = fmt::format("ERROR FlatbufferReader for ID [{}] exists already",
                         FlatbufferID);
    throw std::runtime_error(s);
  }
  m[FlatbufferID] = std::move(item);
}
} // namespace FlatbufferReaderRegistry
} // namespace FileWriter
