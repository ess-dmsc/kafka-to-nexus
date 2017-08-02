#include <cstdio>

#include "Status.hpp"
#include "StatusWriter.hpp"

namespace FileWriter {
namespace Status {

void StdIOWriter::write(const StreamMasterStatus &data) {
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
            << "\t\tsize average :\t" << x.size_avg << "\n"
            << "\t\tsize std :\t" << x.size_std << "\n"
            << "\t\tfrequency average :\t" << x.freq_avg << "\n"
            << "\t\tfrequency std :\t\t" << x.freq_std << "\n";
}

rapidjson::Document JSONWriter::write(const StreamMasterStatus &data) {
  using namespace rapidjson;
  Document d;
  auto &a = d.GetAllocator();
  d.SetObject();
  { // stream master info
    Value sm;
    sm.SetObject();
    sm.AddMember("status", data.status, a);
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

rapidjson::Value JSONWriter::to_json(const StreamerStatusType &x,
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

rapidjson::Value JSONWriter::to_json(const StreamerStatisticsType &x,
                                     return_type &d) {
  using namespace rapidjson;
  auto &a = d.GetAllocator();

  Value value;
  value.SetObject();
  {
    Value size;
    size.SetObject();
    size.AddMember("avg", x.size_avg, a);
    size.AddMember("std", x.size_std, a);
    value.AddMember("size", size, a);
  }
  {
    Value freq;
    freq.SetObject();
    freq.AddMember("avg", x.freq_avg, a);
    freq.AddMember("std", x.freq_std, a);
    value.AddMember("freq", freq, a);
  }
  return value;
}

flatbuffers::Offset<StatusInfo>
FlatbuffersWriter::write(const StreamMasterStatus &data) {

  flatbuffers::FlatBufferBuilder builder(1024);
  std::vector<flatbuffers::Offset<StreamerInfo>> streamers;

  for (size_t i = 0; i < data.topic.size(); ++i) {
    auto t = builder.CreateString(data.topic[i]);
    auto msg_size = Statistics(data.streamer_stats[i].size_avg,
                               data.streamer_stats[i].size_std);
    auto msg_freq = Statistics(data.streamer_stats[i].freq_avg,
                               data.streamer_stats[i].freq_std);
    auto s = CreateStreamerInfo(
        builder, t, int(data.streamer_status[i].messages),
        int(data.streamer_status[i].bytes), int(data.streamer_status[i].errors),
        &msg_size, &msg_freq);
    streamers.push_back(s);
  }
  auto fbs =
      CreateStatusInfo(builder, data.status, builder.CreateVector(streamers));
  FinishStatusInfoBuffer(builder, fbs);
  builder.Clear();
  return fbs;
}

} // namespace Status

} // namespace FileWriter
