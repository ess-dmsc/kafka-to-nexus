#pragma once

#include <memory>
#include <array>
#include <vector>
#include <map>
#include "HDFFile.h"
#include "logger.h"

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {

using FBID = std::array<char, 4>;

FBID fbid_from_str(char const * x);

class SchemaInfo {
public:
typedef std::unique_ptr<SchemaInfo> ptr;
virtual FBSchemaReader::ptr create_reader() = 0;
virtual FBSchemaWriter::ptr create_writer() = 0;
};

class SchemaRegistry {
public:
static std::map<FBID, SchemaInfo::ptr> & items();
static SchemaInfo::ptr & find(FBID const & fbid);

static void registrate(FBID fbid, SchemaInfo::ptr && si) {
	items()[fbid] = std::move(si);
}

template <typename T>
class Registrar {
public:
Registrar(FBID fbid) {
	SchemaRegistry::registrate(fbid, std::unique_ptr<T>(new T));
}
};

};

}
}
}
