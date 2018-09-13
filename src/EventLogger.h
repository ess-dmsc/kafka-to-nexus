#pragma once

#include "FileWriterTask.h"

namespace KafkaW {
class ProducerTopic;
}

namespace FileWriter {
enum class StatusCode : int {
  Start = 0,
  Close = 1,
  Error = 2,
  Fail = 3,
};
std::string convertStatusCodeToString(StatusCode Code);

void logEvent(std::shared_ptr<KafkaW::ProducerTopic> p, StatusCode Code,
              const std::string &ServiceId, const std::string &JobId,
              const std::string &Message);

class EventLogger {
public:
  void create(std::shared_ptr<KafkaW::ProducerTopic> p,
              const std::string &ServiceId, const std::string &JobId);
  void log(StatusCode Code, const std::string &Message);

private:
  std::shared_ptr<KafkaW::ProducerTopic> Producer;
  std::string createEventMessage(StatusCode Code, const std::string &Message);
  nlohmann::json EventMsg;
};
}
