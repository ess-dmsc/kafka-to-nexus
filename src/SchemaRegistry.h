#pragma once

#include "HDFFile.h"
#include "logger.h"
#include <array>
#include <map>
#include <memory>
#include <vector>

namespace FileWriter {
namespace Schemas {

using FBID = std::array<char, 4>;

FBID fbid_from_str(char const *x);

class SchemaInfo {
public:
  typedef std::unique_ptr<SchemaInfo> ptr;
  virtual FBSchemaReader::ptr create_reader() = 0;
};

class SchemaRegistry {
public:
  static std::map<FBID, SchemaInfo::ptr> &items();
  static SchemaInfo::ptr &find(FBID const &fbid);

  static void registrate(FBID fbid, SchemaInfo::ptr &&si) {
    auto &m = items();
    if (m.find(fbid) != m.end()) {
      auto s = fmt::format("ERROR schema handler for [{:.{}}] exists already",
                           fbid.data(), fbid.size());
      throw std::runtime_error(s);
    }
    m[fbid] = std::move(si);
  }

  template <typename T> class Registrar {
  public:
    Registrar(FBID fbid) {
      SchemaRegistry::registrate(fbid, std::unique_ptr<T>(new T));
    }
  };
};

} // namespace Schemas
} // namespace FileWriter
