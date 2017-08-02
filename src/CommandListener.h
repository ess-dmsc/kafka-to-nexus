#pragma once

#include "KafkaW.h"
#include "MainOpt.h"
#include "Master_handler.h"
#include "uri.h"
#include <thread>

namespace FileWriter {

class MessageCallback {
public:
  virtual void operator()(int partition, std::string const &topic,
                          std::string const &msg) = 0;
};

class PollStatus {
public:
  static PollStatus Ok();
  static PollStatus Err();
  static PollStatus make_CmdMsg(std::unique_ptr<CmdMsg> x);
  PollStatus(PollStatus &&);
  PollStatus &operator=(PollStatus &&);
  ~PollStatus();
  void reset();
  PollStatus();
  bool is_Ok();
  bool is_Err();
  std::unique_ptr<CmdMsg> is_CmdMsg();

private:
  int state = -1;
  void *data = nullptr;
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
