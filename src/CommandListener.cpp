#include "CommandListener.h"
#include "KafkaW/ConsumerMessage.h"
#include "Msg.h"
#include "helper.h"
#include "logger.h"
#include <iostream>

namespace FileWriter {

using std::string;
using std::vector;

CommandListener::CommandListener(MainOpt &config) : config(config) {}

CommandListener::~CommandListener() {}

void CommandListener::start() {
  KafkaW::BrokerSettings BrokerSettings;
  BrokerSettings.PollTimeoutMS = 500;
  BrokerSettings.Address = config.command_broker_uri.HostPort;
  BrokerSettings.KafkaConfiguration["group.id"] = fmt::format(
      "filewriter--commandhandler--host:{}--pid:{}--topic:{}--time:{}",
      gethostname_wrapper(), getpid_wrapper(), config.command_broker_uri.Topic,
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  consumer.reset(new KafkaW::Consumer(BrokerSettings));
  consumer->addTopic(config.command_broker_uri.Topic);
}

void CommandListener::poll(KafkaW::PollStatus &Status, std::unique_ptr<Msg> Message) {
  consumer->poll(Status, std::move(Message));
}

} // namespace FileWriter
