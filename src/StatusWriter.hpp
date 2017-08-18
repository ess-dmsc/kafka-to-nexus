#pragma once
// #include "schemas/fws0_fwr_status_generated.h"

#if RAPIDJSON_HAS_STDSTRING == 0
#undef RAPIDJSON_HAS_STDSTRING
#define RAPIDJSON_HAS_STDSTRING 1
#endif
#include "rapidjson/document.h"

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

protected:
  return_type write_impl(const StreamMasterStatus &);
  rapidjson::Value to_json(const StreamerStatusType &, return_type &);
  rapidjson::Value to_json(const StreamerStatisticsType &, return_type &);
};

class JSONStreamWriter : public JSONWriter {

public:
  using return_type = std::string;
  return_type write(const StreamMasterStatus &);
};

template <class W> typename W::return_type pprint(const StreamMasterStatus &x) {
  W writer;
  return writer.write(x);
}

} // namespace Status
} // namespace FileWriter
