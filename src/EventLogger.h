// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FileWriterTask.h"
#include <chrono>
#include <functional>

namespace KafkaW {
class ProducerTopic;
}

namespace FileWriter {
/// \brief Enum class to map NICOS status code values.
///
/// Close represent the successful closure of a NeXus file, Error a generic, non
/// critical error, Fail the impossibility to start the job.
enum class StatusCode : int {
  Start = 0,
  Close = 1,
  Error = 2,
  Fail = 3,
};

/// \brief Convert the StatusCode into a string.
///
/// \param Code The StatusCode.
///
/// \return An upper case string that NICOS can interpret.
std::string convertStatusCodeToString(StatusCode Code);

/// \brief Builds a JSON string that contains an event log and emits the
/// log via the producer.
///
/// The event log is described by a code ("START","CLOSE","FAIL","ERROR"), the
/// service and job ids, the system timestamp at which the log has been emitted
/// and a message.
///
/// \param Producer The producer, for example KafkaW::ProducerTopic.
/// \param Code The code.
/// \param ServiceId The service identifier.
/// \param JobId The job identifier.
/// \param Message The message.
///
/// \tparam     ProducerType  { description }
template <class ProducerType>
void logEvent(std::shared_ptr<ProducerType> Producer, StatusCode Code,
              const std::string &ServiceId, const std::string &JobId,
              const std::string &Message) {

  if (!Producer) {
    getLogger()->error("Can't produce filewriter event log, invalid Producer");
    return;
  }

  std::chrono::system_clock::time_point Now = std::chrono::system_clock::now();
  nlohmann::json Event;
  Event["type"] = "filewriter_event";
  Event["code"] = convertStatusCodeToString(Code);
  Event["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                           Now.time_since_epoch())
                           .count();
  Event["service_id"] = ServiceId;
  Event["job_id"] = JobId;
  Event["message"] = Message;

  std::string EventMessage = Event.dump();
  Producer->produce(EventMessage);
}
} // namespace FileWriter
