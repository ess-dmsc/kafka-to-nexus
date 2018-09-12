#pragma once

#include "FileWriterTask.h"

namespace KafkaW {
class ProducerTopic;
}

namespace FileWriter {
class EventLogger {
public:
  void connect(std::shared_ptr<KafkaW::ProducerTopic> p,
               const std::string &ServiceId, const std::string &JobId);
  void log(const std::string &Code, const std::string &Message);

private:
  std::shared_ptr<KafkaW::ProducerTopic> Producer;
  std::string createEventMessage(const std::string &Code,
                                 const std::string &Message);
};
}
