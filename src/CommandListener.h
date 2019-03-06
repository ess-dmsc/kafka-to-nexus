#pragma once

#include "KafkaW/Consumer.h"
#include "MainOpt.h"
#include "URI.h"
#include <thread>

namespace FileWriter {

/// Check for new commands on the topic, return them to the Master.
class CommandListener {
public:
  explicit CommandListener(MainOpt &config);
  ~CommandListener() = default;

  /// Start listening to command messages.
  void start();
  void stop();

  /// Check for new command packets and return one if there is.
  std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> poll();

private:
  MainOpt &config;
  std::unique_ptr<KafkaW::ConsumerInterface> consumer;
  std::shared_ptr<spdlog::logger> Logger = spdlog::get("filewriterlogger");
};
} // namespace FileWriter
