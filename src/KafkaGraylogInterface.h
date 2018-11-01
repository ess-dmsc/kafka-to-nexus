#pragma once

#include "graylog_logger/LogUtil.hpp"
#include <thread>

class KafkaGraylogInterface : public Log::BaseLogHandler {
public:
  explicit KafkaGraylogInterface(std::string BrokerAddress, std::string Topic,
                                 const size_t MaxQueueLength = 100);
  KafkaGraylogInterface();

protected:
  void exitThread();
  std::string Broker;
  std::string TopicName;
  void threadFunction();
  std::thread KafkaGraylogThread;
};
