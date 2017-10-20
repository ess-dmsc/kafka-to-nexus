#include <cstdio>

#include "Status.hpp"
#include "StatusWriter.hpp"

#include "rapidjson/filewritestream.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/writer.h"

namespace FileWriter {
namespace Status {

rapidjson::Document JSONWriterBase::write_impl(StreamMasterInfo &info) const {
  using namespace rapidjson;
  Document d;
  auto &a = d.GetAllocator();
  d.SetObject();

  auto next_message_relative_eta = info.time_to_next_message();
  { // message type
    d.AddMember("type", "stream_master_status", a);
    d.AddMember("next_message_eta", next_message_relative_eta, a);
  }
  { // stream master info
    Value sm;
    sm.SetObject();
    sm.AddMember("state", StringRef(Err2Str(info.status())), a);
    sm.AddMember("status", primary_quantities(info.total(), a), a);
    sm.AddMember("statistics", derived_quantities(info.total(), next_message_relative_eta, a), a);
    d.AddMember("stream_master", sm, a);
  }
  { // streamers info
    Value ss;
    ss.SetObject();
    for (auto &topic : info.info()) {
      Value key{topic.first.c_str(), a};
      Value val;
      val.SetObject();
      val.AddMember("status", primary_quantities(topic.second, a), a);
      val.AddMember("statistics", derived_quantities(topic.second, next_message_relative_eta, a), a);
      ss.AddMember(key, val, a);
    }
    d.AddMember("streamer", ss, a);
  }
  return d;
}

template <class Allocator>
rapidjson::Value JSONWriterBase::primary_quantities(MessageInfo &info,
                                                    Allocator &a) const {
  using namespace rapidjson;
  Value value;
  value.SetObject();
  value.AddMember("messages", info.messages().first, a);
  value.AddMember("Mbytes", info.Mbytes().first, a);
  value.AddMember("errors", info.errors(), a);
  return value;
}

template <class Allocator>
rapidjson::Value create_derived_quantity(MessageInfo::value_type &value,
                                         Allocator &a) {
  rapidjson::Value result;
  result.SetObject();
  result.AddMember("average", value.first, a);
  result.AddMember("stdandard_deviation", value.second, a);
  return std::move(result);
}

template <class Allocator>
rapidjson::Value JSONWriterBase::derived_quantities(MessageInfo &info,
                                                    double duration,
                                                    Allocator &a) const {
  using namespace rapidjson;
  auto size = message_size(info);
  auto frequency = FileWriter::Status::message_frequency(info, duration);
  auto throughput = FileWriter::Status::message_throughput(info, duration);

  Value value;
  value.SetObject();

  value.AddMember("size", create_derived_quantity(size, a), a);
  value.AddMember("frequency", create_derived_quantity(frequency, a), a);
  value.AddMember("throughput", create_derived_quantity(throughput, a), a);

  return value;
}

JSONStreamWriter::return_type
JSONStreamWriter::write(StreamMasterInfo &info) const {
  auto value = base.write_impl(info);
  rapidjson::StringBuffer buffer;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> w(buffer);
  w.SetMaxDecimalPlaces(1);
  value.Accept(w);
  std::string s{buffer.GetString()};
  return std::move(s);
}

JSONWriter::return_type JSONWriter::write(StreamMasterInfo &info) const {
  return base.write_impl(info);
}

} // namespace Status

} // namespace FileWriter
