#pragma once

#if RAPIDJSON_HAS_STDSTRING == 0
#undef RAPIDJSON_HAS_STDSTRING
#define RAPIDJSON_HAS_STDSTRING 1
#endif
#include "rapidjson/document.h"

namespace FileWriter {
namespace Status {

struct StreamMasterStatus;
struct StreamerStatusType;
struct StreamerStatisticsType;

class JSONWriter;
class JSONStreamWriter;

class StdIOWriter {
public:
  using return_type = void;
  return_type write(const StreamMasterStatus &);

private:
  void print(const StreamerStatusType &);
  void print(const StreamerStatisticsType &);
};

class JSONWriterBase {
  friend class JSONWriter;
  friend class JSONStreamWriter;

private:
  using return_type = rapidjson::Document;

  return_type write_impl(const StreamMasterStatus &);
  rapidjson::Value to_json(const StreamerStatusType &, return_type &);
  rapidjson::Value to_json(const StreamerStatisticsType &, return_type &);
};

class JSONWriter {
public:
  using return_type = rapidjson::Document;
  return_type write(const StreamMasterStatus &);

private:
  JSONWriterBase base;
};

class JSONStreamWriter {
public:
  using return_type = std::string;
  return_type write(const StreamMasterStatus &);

private:
  JSONWriterBase base;
};

template <class W> typename W::return_type pprint(const StreamMasterStatus &x) {
  W writer;
  return writer.write(x);
}

} // namespace Status
} // namespace FileWriter
