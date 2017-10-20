#pragma once

#if RAPIDJSON_HAS_STDSTRING == 0
#undef RAPIDJSON_HAS_STDSTRING
#define RAPIDJSON_HAS_STDSTRING 1
#endif
#include "rapidjson/document.h"

namespace FileWriter {
namespace Status {

class StreamMasterInfo;
class MessageInfo;

class JSONWriterBase {
  friend class JSONWriter;
  friend class JSONStreamWriter;
  friend class StdIOWriter;

private:
  using return_type = rapidjson::Document;

  return_type write_impl(StreamMasterInfo &) const;
  template <class Allocator>
  rapidjson::Value primary_quantities(MessageInfo &, Allocator &) const;
  template <class Allocator>
  rapidjson::Value derived_quantities(MessageInfo &, double, Allocator &) const;
};

class StdIOWriter {
public:
  using return_type = void;
  return_type write(StreamMasterInfo &) const;

private:
  JSONWriterBase base;
};

class JSONWriter {
public:
  using return_type = rapidjson::Document;
  return_type write(StreamMasterInfo &) const;

private:
  JSONWriterBase base;
};

class JSONStreamWriter {
public:
  using return_type = std::string;
  return_type write(StreamMasterInfo &) const;

private:
  JSONWriterBase base;
};

template <class W> typename W::return_type pprint(StreamMasterInfo &x) {
  W writer;
  return writer.write(x);
}

} // namespace Status
} // namespace FileWriter
