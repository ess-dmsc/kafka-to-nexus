#pragma once

#include "KafkaW/KafkaW.h"
#include "MainOpt.h"
#include "uri.h"
#include <thread>

namespace FileWriter {

class MessageCallback {
public:
  virtual void operator()(int partition, std::string const &topic,
                          std::string const &msg) = 0;
};

/// Check for new commands on the topic, return them to the Master.
class CommandListener {
public:
  CommandListener(MainOpt &config);
  ~CommandListener();
  /// Start listening to command messages.
  void start();
  void stop();
  /// Check for new command packets and return one if there is.
  KafkaW::PollStatus poll();

private:
  MainOpt &config;
  std::unique_ptr<KafkaW::Consumer> consumer;
};

} // namespace FileWriter
