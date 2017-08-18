#include <cstdio>

#include "Status.hpp"
#include "StatusWriter.hpp"

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace FileWriter {
namespace Status {

StdIOWriter::return_type StdIOWriter::write(const StreamMasterStatus &data) {
  std::cout << "stream_master :\t" << data.status << "\n";
  for (size_t i = 0; i < data.topic.size(); ++i) {
    std::cout << data.topic[i] << ":\n";
    print(data.streamer_status[i]);
    print(data.streamer_stats[i]);
    std::cout << "\n";
  }
}

void StdIOWriter::print(const StreamerStatusType &x) {
  std::cout << "\tstatus :\n"
            << "\t\tmessages :\t" << x.messages << "\n"
            << "\t\tbytes :\t\t" << x.bytes << "\n"
            << "\t\terrors :\t" << x.errors << "\n";
}
void StdIOWriter::print(const StreamerStatisticsType &x) {
  std::cout << "\tstatistics :\n"
            << "\t\tsize average :\t" << x.average_message_size << "\n"
            << "\t\tsize std :\t" << x.standard_deviation_message_size << "\n"
            << "\t\tfrequency average :\t" << x.average_message_frequency
            << "\n"
            << "\t\tfrequency std :\t\t"
            << x.standard_deviation_message_frequency << "\n";
}

rapidjson::Document JSONWriterBase::write_impl(const StreamMasterStatus &data) {
  using namespace rapidjson;
  Document d;
  auto &a = d.GetAllocator();
  d.SetObject();
  { // message type
    d.AddMember("type", "filewriter_streammaster_status", a);
  }
  { // stream master info
    Value sm, ss;
    sm.SetObject();
    std::string s{Err2Str(StreamMasterError{data.status})};
    sm.AddMember("status", StringRef(s), a);
    d.AddMember("streammaster", sm, a);
  }
  { // streamers info
    Value ss;
    ss.SetObject();
    for (size_t i = 0; i < data.topic.size(); ++i) {
      Value key{data.topic[i].c_str(), a};
      Value val;
      val.SetObject();
      val.AddMember("status", to_json(data.streamer_status[i], d), a);
      val.AddMember("statistics", to_json(data.streamer_stats[i], d), a);
      ss.AddMember(key, val, a);
    }
    d.AddMember("streamer", ss, a);
  }
  return d;
}

rapidjson::Value JSONWriterBase::to_json(const StreamerStatusType &x,
                                     return_type &d) {
  using namespace rapidjson;
  auto &a = d.GetAllocator();
  Value value;
  value.SetObject();
  value.AddMember("messages", int(x.messages), a);
  value.AddMember("bytes", int(x.bytes), a);
  value.AddMember("errors", int(x.errors), a);
  return value;
}

rapidjson::Value JSONWriterBase::to_json(const StreamerStatisticsType &x,
                                     return_type &d) {
  using namespace rapidjson;
  auto &a = d.GetAllocator();

  Value value;
  value.SetObject();
  {
    Value size;
    size.SetObject();
    size.AddMember("avg", x.average_message_size, a);
    size.AddMember("std", x.standard_deviation_message_size, a);
    value.AddMember("size", size, a);
  }
  {
    Value freq;
    freq.SetObject();
    freq.AddMember("avg", x.average_message_frequency, a);
    freq.AddMember("std", x.standard_deviation_message_frequency, a);
    value.AddMember("freq", freq, a);
  }
  return value;
}

JSONWriter::return_type JSONWriter::write(const StreamMasterStatus &data) {
  return write_impl(data);
}

JSONStreamWriter::return_type
JSONStreamWriter::write(const StreamMasterStatus &data) {
  auto value = write_impl(data);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> w(buffer);
  value.Accept(w);
  std::string s{buffer.GetString()};
  return std::move(s);
}

} // namespace Status

} // namespace FileWriter
