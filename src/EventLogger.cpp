#include "EventLogger.h"
#include "Report.h"

class EventLogger {
public:
  void connect(std::shared_ptr<KafkaW::ProducerTopic> p,
               const std::string &ServiceId, const std::string &JobId) {
    Producer = p;
    EventMsg["type"] = "filewriter_event";
    EventMsg["service_id"] = ServiceId;
    EventMsg["job_id"] = JobId;
  }
  void log(const std::string &Code, const std::string &Message) {
    std::string EventMessage{createEventMessage(Code, Message)};
    if (!Producer) {
      LOG(Sev::Warning,
          "Can't produce filewriter event log, invalid KafkaW::ProducerTopic");
    }
    Producer->produce(reinterpret_cast<unsigned char *>(&EventMessage[0]),
                      EventMessage.size());
  }

private:
  std::shared_ptr<KafkaW::ProducerTopic> Producer{nullptr};
  nlohmann::json EventMsg;

  std::string createEventMessage(const std::string &Code,
                                 const std::string &Message) {
    EventMsg["code"] = Code;
    EventMsg["message"] = Message;
    EventMsg["timestamp"] = 0;
    return EventMsg.dump();
  }
};
