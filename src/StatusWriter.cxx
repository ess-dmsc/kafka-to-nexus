#include "StatusWriter.h"
#include "Status.h"

namespace FileWriter {
namespace Status {

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
  std::pair<double, double> Size = messageSize(Informations);
  double Frequency =
      FileWriter::Status::messageFrequency(Informations, SinceLastMessage);
  double Throughput =
      FileWriter::Status::messageThroughput(Informations, SinceLastMessage);

  nlohmann::json Status = {"status",
                           {{"messages", Informations.getMessages().first},
                            {"Mbytes", Informations.getMbytes().first},
                            {"errors", Informations.getErrors()}}};

  nlohmann::json Statistics = {
      "statistics",
      {{"size",
        {{"average", Size.first}, {"stdandard_deviation", Size.second}}},
       {"frequency", Frequency},
       {"throughput", Throughput}}};

  return nlohmann::json{Status, Statistics};
}

NLWriterBase::NLWriterBase() {
  json = {{"type", "stream_master_status"},
          {"next_message_eta_ms", 0},
          {"job_id", 0}};
}

void NLWriterBase::setJobId(const std::string &JobId) {
  json["job_id"] = JobId;
}

void NLWriterBase::write(StreamMasterInfo &Informations) {
  json["next_message_eta_ms"] = Informations.getTimeToNextMessage().count();
  json["stream_master"] = StreamMasterToJson(Informations);
}

void NLWriterBase::write(MessageInfo &Informations, const std::string &Topic,
                         const std::chrono::milliseconds &SinceLastMessage) {
  json["streamer"][Topic] = StreamerToJson(Informations, SinceLastMessage);
}

} // namespace Status

} // namespace FileWriter
