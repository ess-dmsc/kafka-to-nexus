#include "HDFWriterModule.h"

std::map<std::string, TV> &HDFWriterModule::items() {
  static std::map<std::string, TV> _items;
  return _items;
}

SchemaRegistry::TV &SchemaRegistry::find(std::string const &key) {
  static SchemaInfo::ptr empty;
  auto &_items = items();
  auto f = _items.find(fbid);
  if (f == _items.end()) {
    return nullptr;
  }
  return f->second;
}
}
