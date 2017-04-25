#include "SchemaRegistry.h"
#include "logger.h"

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {

FBID fbid_from_str(char const *x) {
  FBID ret;
  memcpy(ret.data(), x, 4);
  return ret;
}

std::map<FBID, SchemaInfo::ptr> &SchemaRegistry::items() {
  static std::map<FBID, SchemaInfo::ptr> _items;
  return _items;
}

SchemaInfo::ptr &SchemaRegistry::find(FBID const &fbid) {
  static SchemaInfo::ptr empty;
  auto &_items = items();
  auto f = _items.find(fbid);
  if (f == _items.end())
    return empty;
  return f->second;
}

} // namespace Schemas
} // namespace FileWriter
} // namespace BrightnESS
