#include <cstdio>

#include "Status.h"
#include "StatusWriter.h"

#include "rapidjson/filewritestream.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/writer.h"

namespace FileWriter {
namespace Status {
NLWriterBase::NLWriterBase() {
  json = {{"type", "stream_master_status"},
          {"next_message_eta_ms", 0},
          {"job_id", 0}};
}

nlohmann::json StreamMasterToJson(StreamMasterInfo &Informations) {
  nlohmann::json Value = {{"state", Err2Str(Informations.StreamMasterStatus)},
                          {"messages", Informations.getMessages().first},
                          {"Mbytes", Informations.getMbytes().first},
                          {"errors", Informations.getErrors()},
                          {"runtime", Informations.runTime().count()}};
  return Value;
}

nlohmann::json
StreamerToJson(MessageInfo &Informations,
               const std::chrono::milliseconds &SinceLastMessage) {
  auto Size = messageSize(Informations);
  auto Frequency =
      FileWriter::Status::messageFrequency(Informations, SinceLastMessage);
  auto Throughput =
      FileWriter::Status::messageThroughput(Informations, SinceLastMessage);

  nlohmann::json Status = {"status",
                           {{"messages", Informations.getMessages().first},
                            {"Mbytes", Informations.getMbytes().first},
                            {"errors", Informations.getErrors()}}};

  nlohmann::json Statistics = {
      "statistics",
      {{"size",
        {{"average", Size.first}, {"stdandard_deviation", Size.second}}},
       {"frequency",
        {{"average", Frequency.first},
         {"stdandard_deviation", Frequency.second}}},
       {"throughput",
        {{"average", Throughput.first},
         {"stdandard_deviation", Throughput.second}}}}};

  return nlohmann::json{Status, Statistics};
}

void NLWriterBase::write(StreamMasterInfo &Informations) {
  json["next_message_eta_ms"] = Informations.getTimeToNextMessage().count();
  json["stream_master"] = StreamMasterToJson(Informations);
};

void NLWriterBase::write(MessageInfo &Informations, const std::string &Topic,
                         const std::chrono::milliseconds &SinceLastMessage) {
  json["streamer"][Topic] = StreamerToJson(Informations, SinceLastMessage);
}

void NLJSONWriter::write(StreamMasterInfo &Informations) {
  Base.write(Informations);
}
void NLJSONWriter::write(MessageInfo &Informations, const std::string &Topic,
                         const std::chrono::milliseconds &SinceLastMessage) {
  Base.write(Informations, Topic, SinceLastMessage);
}

nlohmann::json NLJSONWriter::get() { return Base.json; }

} // namespace Status

} // namespace FileWriter
