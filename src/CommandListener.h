#pragma once

#include "KafkaW/KafkaW.h"
#include "MainOpt.h"
#include "uri.h"
#include <thread>

namespace FileWriter {

/// Check for new commands on the topic, return them to the Master.
class CommandListener {
public:
  explicit CommandListener(MainOpt &config);
  ~CommandListener();

  /// Start listening to command messages.
  void start();
  void stop();

  /// Check for new command packets and return one if there is.
  std::unique_ptr<KafkaW::ConsumerMessage> poll();

private:
  MainOpt &config;
  std::unique_ptr<KafkaW::ConsumerInterface> consumer;
};
} // namespace FileWriter
