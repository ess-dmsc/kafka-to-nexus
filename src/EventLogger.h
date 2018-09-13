#pragma once

#include "FileWriterTask.h"

namespace KafkaW {
class ProducerTopic;
}

namespace FileWriter {
class EventLogger {
public:
  enum class Severity : int {
    Start = 0,
    Close = 1,
    Error = 2,
    Fail = 3,
  };

  void connect(std::shared_ptr<KafkaW::ProducerTopic> p,
               const std::string &ServiceId, const std::string &JobId);
  void log(Severity Level, const std::string &Message);

private:
  std::shared_ptr<KafkaW::ProducerTopic> Producer;
  std::string createEventMessage(Severity Level, const std::string &Message);
  std::string convertSeverityToString(Severity Level);
  nlohmann::json EventMsg;
};
}
