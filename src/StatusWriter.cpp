#include "StatusWriter.h"
#include "Status.h"

namespace FileWriter {
namespace Status {

nlohmann::json StreamMasterToJson(StreamMasterInfo &Information) {
  nlohmann::json Value = {{"state", Err2Str(Information.StreamMasterStatus)},
                          {"messages", Information.getMessages()},
                          {"Mbytes", Information.getMbytes()},
                          {"errors", Information.getErrors()},
                          {"runtime", Information.runTime().count()}};
  return Value;
}

nlohmann::json StreamerToJson(MessageInfo &Information) {
  std::pair<double, double> Size = Information.messageSize();

  nlohmann::json Status;
  Status["rates"] = {
      {"messages", Information.getMessages()},
      {"Mbytes", Information.getMbytes()},
      {"errors", Information.getErrors()},
      {"message_size",
       {{"average", Size.first}, {"standard_deviation", Size.second}}}};

  return Status;
} // namespace Status

void StatusWriter::setJobId(const std::string &JobId) {
  json["job_id"] = JobId;
}

void StatusWriter::write(StreamMasterInfo &Information) {
  json["next_message_eta_ms"] = Information.getTimeToNextMessage().count();
  json["stream_master"] = StreamMasterToJson(Information);
  json["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now().time_since_epoch())
                          .count();
}

void StatusWriter::write(MessageInfo &Information, const std::string &Topic) {
  json["streamer"][Topic] = StreamerToJson(Information);
}

std::string StatusWriter::getJson() {
  // Indent using 4 spaces
  return json.dump(4);
}

} // namespace Status
} // namespace FileWriter
