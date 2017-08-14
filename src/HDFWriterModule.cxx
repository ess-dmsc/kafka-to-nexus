#include "HDFWriterModule.h"

namespace FileWriter {

std::map<HDFWriterModuleRegistry::K, HDFWriterModuleRegistry::V> &
HDFWriterModuleRegistry::items() {
  static std::map<HDFWriterModuleRegistry::K, HDFWriterModuleRegistry::V>
      _items;
  return _items;
}

HDFWriterModuleRegistry::V &
HDFWriterModuleRegistry::find(HDFWriterModuleRegistry::K const &key) {
  static HDFWriterModuleRegistry::V empty;
  auto &_items = items();
  auto f = _items.find(key);
  if (f == _items.end()) {
    return empty;
  }
  return f->second;
}
}
