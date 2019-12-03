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

#include <chrono>

namespace FileWriter {
namespace Status {

nlohmann::json StreamMasterToJson(StreamMasterInfo const &Information) {
  nlohmann::json Value = {
      {"state", Err2Str(Information.StreamMasterStatus)},
      {"messages", Information.getNumberMessages()},
      {"processed", Information.getNumberProcessedMessages()},
      {"Mbytes", Information.getMbytes()},
      {"errors", Information.getNumberErrors()},
      {"validate_errors", Information.getNumberValidationErrors()},
      {"runtime", Information.runTime().count()}};
  return Value;
}

nlohmann::json StreamerToJson(MessageInfo const &Information) {
  nlohmann::json Status;
  Status["rates"] = {
      {"messages", Information.getNumberMessages()},
      {"processed", Information.getNumberProcessedMessages()},
      {"Mbytes", Information.getMbytes()},
      {"errors", Information.getNumberWriteErrors()},
      {"validate_errors", Information.getNumberValidationErrors()}};

  return Status;
}

void StatusWriter::setJobId(const std::string &JobId) {
  json["job_id"] = JobId;
}

void StatusWriter::write(StreamMasterInfo &Information) {
  json["next_message_eta_ms"] = Information.getTimeToNextMessage().count();
  json["stream_master"] = StreamMasterToJson(Information);
  json["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
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
