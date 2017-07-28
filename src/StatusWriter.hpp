#pragma once
#include <rapidjson/document.h>

namespace FileWriter {
namespace Status {

class StreamMasterStatus;
class StreamerStatusType;
class StreamerStatisticsType;

class StdIOWriter {
public:
  using return_type = void;

  return_type write(const StreamMasterStatus &);

private:
  void print(const StreamerStatusType &);
  void print(const StreamerStatisticsType &);
};

class JSONWriter {

public:
  using return_type = rapidjson::Document;
  return_type write(const StreamMasterStatus &);

private:
  rapidjson::Value to_json(const StreamerStatusType &, return_type &);
  rapidjson::Value to_json(const StreamerStatisticsType &, return_type &);
};

class FlatbuffersWriter {

public:
  using return_type = void;
  return_type write(const StreamMasterStatus &);

private:
};

template <class W> struct print_value { using type = void; };
template <> struct print_value<JSONWriter> {
  using type = rapidjson::Document;
};

template <class W>
typename print_value<W>::type pprint(const StreamMasterStatus &x);

template <>
typename StdIOWriter::return_type
pprint<StdIOWriter>(const StreamMasterStatus &x);
template <>
typename JSONWriter::return_type
pprint<JSONWriter>(const StreamMasterStatus &x);

} // namespace Status
} // namespace FileWriter
