#include "EventLogger.h"
#include "Report.h"

void FileWriter::logEvent(std::shared_ptr<KafkaW::ProducerTopic> Producer,
                          FileWriter::StatusCode Code,
                          const std::string &ServiceId,
                          const std::string &JobId,
                          const std::string &Message) {
  if (!Producer) {
    LOG(Sev::Warning,
        "Can't produce filewriter event log, invalid KafkaW::ProducerTopic");
    return;
  }

  nlohmann::json Event;
  Event["type"] = "filewriter_event";
  Event["code"] = convertStatusCodeToString(Code);
  Event["timestamp"] = 0;
  Event["service_id"] = ServiceId;
  Event["job_id"] = JobId;
  Event["message"] = Message;

  std::string EventMessage = Event.dump();
  Producer->produce(reinterpret_cast<unsigned char *>(&EventMessage[0]),
                    EventMessage.size());
}

std::string FileWriter::convertStatusCodeToString(FileWriter::StatusCode Code) {
  switch (Code) {
  case FileWriter::StatusCode::Start:
    return "START";
    break;
  case FileWriter::StatusCode::Close:
    return "CLOSE";
    break;
  case FileWriter::StatusCode::Error:
    return "ERROR";
    break;
  case FileWriter::StatusCode::Fail:
    return "FAIL";
    break;
  default:
    return "UNKNOWN";
    break;
  }
  return "";
};

void FileWriter::EventLogger::create(std::shared_ptr<KafkaW::ProducerTopic> p,
                                     const std::string &ServiceId,
                                     const std::string &JobId) {}
void FileWriter::EventLogger::log(FileWriter::StatusCode Code,
                                  const std::string &Message) {}

std::string
FileWriter::EventLogger::createEventMessage(FileWriter::StatusCode Code,
                                            const std::string &Message) {
  return "";
}
