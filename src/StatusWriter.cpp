#include "StatusWriter.h"
#include "Status.h"

namespace FileWriter {
namespace Status {

nlohmann::json StreamMasterToJson(StreamMasterInfo &Information) {
  nlohmann::json Value = {{"state", Err2Str(Information.StreamMasterStatus)},
                          {"messages", Information.getMessages().first},
                          {"Mbytes", Information.getMbytes().first},
                          {"errors", Information.getErrors()},
                          {"runtime", Information.runTime().count()}};
  return Value;
}

nlohmann::json
StreamerToJson(MessageInfo &Information,
               const std::chrono::milliseconds &SinceLastMessage) {
  std::pair<double, double> Size = messageSize(Information);
  double Frequency =
      FileWriter::Status::messageFrequency(Information, SinceLastMessage);
  double Throughput =
      FileWriter::Status::messageThroughput(Information, SinceLastMessage);

  nlohmann::json Status = {"status",
                           {{"messages", Information.getMessages().first},
                            {"Mbytes", Information.getMbytes().first},
                            {"errors", Information.getErrors()}}};

  nlohmann::json Statistics = {
      "statistics",
      {{"size",
        {{"average", Size.first}, {"stdandard_deviation", Size.second}}},
       {"frequency", Frequency},
       {"throughput", Throughput}}};

  return nlohmann::json{Status, Statistics};
}

StatusWriter::StatusWriter() {
  json = {{"type", "stream_master_status"},
          {"next_message_eta_ms", 0},
          {"job_id", 0}};
}

void StatusWriter::setJobId(const std::string &JobId) {
  json["job_id"] = JobId;
}

void StatusWriter::write(StreamMasterInfo &Information) {
  json["next_message_eta_ms"] = Information.getTimeToNextMessage().count();
  json["stream_master"] = StreamMasterToJson(Information);
}

void StatusWriter::write(MessageInfo &Information, const std::string &Topic,
                         const std::chrono::milliseconds &SinceLastMessage) {
  json["streamer"][Topic] = StreamerToJson(Information, SinceLastMessage);
}

std::string StatusWriter::getJson() {
  // Indent using 4 spaces
  return json.dump(4);
}

} // namespace Status
} // namespace FileWriter
