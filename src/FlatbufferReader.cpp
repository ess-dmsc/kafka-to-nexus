// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatbufferReader.h"
#include <stdexcept>

namespace FileWriter::FlatbufferReaderRegistry {

std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> &getReaders() {
  static std::map<std::string, FlatbufferReaderRegistry::ReaderPtr> _items;
  return _items;
}

FlatbufferReaderRegistry::ReaderPtr &find(std::string const &Key) {
  auto &_items = getReaders();
  try {
    return _items.at(Key);
  } catch (std::out_of_range &E) {
    auto s = fmt::format(R"(No such Reader in registry: "{}")", E.what());
    std::throw_with_nested(std::out_of_range(s));
  }
}

void addReader(std::string const &FlatbufferID, FlatbufferReader::ptr &&Item) {
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
  m[FlatbufferID] = std::move(Item);
}
} // namespace FileWriter::FlatbufferReaderRegistry
