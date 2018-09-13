#include "EventLogger.h"
#include "Report.h"

void FileWriter::EventLogger::connect(std::shared_ptr<KafkaW::ProducerTopic> p,
                                      const std::string &ServiceId,
                                      const std::string &JobId) {
  Producer = p;
  EventMsg["type"] = "filewriter_event";
  EventMsg["service_id"] = ServiceId;
  EventMsg["job_id"] = JobId;
}
void FileWriter::EventLogger::log(FileWriter::EventLogger::Severity Level,
                                  const std::string &Message) {
  std::string EventMessage{createEventMessage(Level, Message)};
  if (!Producer) {
    LOG(Sev::Warning,
        "Can't produce filewriter event log, invalid KafkaW::ProducerTopic");
    return;
  }
  Producer->produce(reinterpret_cast<unsigned char *>(&EventMessage[0]),
                    EventMessage.size());
}

std::string FileWriter::EventLogger::createEventMessage(
    FileWriter::EventLogger::Severity Level, const std::string &Message) {
  EventMsg["code"] = convertSeverityToString(Level);
  EventMsg["message"] = Message;
  EventMsg["timestamp"] = 0;
  return EventMsg.dump();
}

std::string FileWriter::EventLogger::convertSeverityToString(
    FileWriter::EventLogger::Severity Level) {
  switch (Level) {
  case FileWriter::EventLogger::Severity::Start:
    break;
  case FileWriter::EventLogger::Severity::Close:
    break;
  case FileWriter::EventLogger::Severity::Error:
    break;
  case FileWriter::EventLogger::Severity::Fail:
    break;
  default:
    break;
  }
  return "";
};
