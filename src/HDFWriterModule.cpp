#include "HDFWriterModule.h"

namespace FileWriter {

namespace HDFWriterModuleRegistry {

std::map<std::string, HDFWriterModuleRegistry::ModuleFactory> &getFactories() {
  static std::map<std::string, HDFWriterModuleRegistry::ModuleFactory> _items;
  return _items;
}

HDFWriterModuleRegistry::ModuleFactory &find(std::string const &key) {
  auto &_items = getFactories();
  return _items.at(key);
}

void addWriterModule(std::string const &Key, ModuleFactory Value) {
  auto &m = getFactories();
  if (Key.size() != 4) {
    throw std::runtime_error(
        "The number of characters in the Flatbuffer id string must be 4.");
  }
  if (m.find(Key) != m.end()) {
    auto s = fmt::format("ERROR entry for key [{}] exists already", Key);
    throw std::runtime_error(s);
  }
  m[Key] = std::move(Value);
}
} // namespace HDFWriterModuleRegistry
} // namespace FileWriter
