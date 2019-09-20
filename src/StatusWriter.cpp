// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "StatusWriter.h"
#include "Status.h"
#include "Utilities.h"

namespace FileWriter {
namespace Status {

nlohmann::json StreamMasterToJson(StreamMasterInfo const &Information) {
  nlohmann::json Value = {{"state", Err2Str(Information.StreamMasterStatus)},
                          {"messages", Information.getMessages()},
                          {"Mbytes", Information.getMbytes()},
                          {"errors", Information.getErrors()},
                          {"runtime", Information.runTime().count()}};
  return Value;
}

nlohmann::json StreamerToJson(MessageInfo const &Information) {
  std::pair<double, double> Size = Information.messageSizeStats();

  nlohmann::json Status;
  Status["rates"] = {
      {"messages", Information.getNumberMessages()},
      {"Mbytes", Information.getMbytes()},
      {"errors", Information.getErrors()},
      {"message_size",
       {{"average", Size.first}, {"standard_deviation", Size.second}}}};

  return Status;
}

void StatusWriter::setJobId(const std::string &JobId) {
  json["job_id"] = JobId;
}

void StatusWriter::write(StreamMasterInfo &Information) {
  json["next_message_eta_ms"] = Information.getTimeToNextMessage().count();
  json["stream_master"] = StreamMasterToJson(Information);
  json["timestamp"] = getCurrentTimeStampMS().count();
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
