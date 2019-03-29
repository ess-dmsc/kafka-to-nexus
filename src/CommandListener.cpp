#include <KafkaW/ConsumerFactory.h>
#include <memory>

#include "CommandListener.h"
#include "KafkaW/PollStatus.h"
#include "Msg.h"
#include "helper.h"
#include "logger.h"

namespace FileWriter {

using std::string;

CommandListener::CommandListener(MainOpt &config) : config(config) {}

void CommandListener::start() {
  KafkaW::BrokerSettings BrokerSettings =
      config.StreamerConfiguration.BrokerSettings;
  BrokerSettings.PollTimeoutMS = 500;
  BrokerSettings.Address = config.CommandBrokerURI.HostPort;
  BrokerSettings.KafkaConfiguration["group.id"] = fmt::format(
      "filewriter--commandhandler--host:{}--pid:{}--topic:{}--time:{}",
      gethostname_wrapper(), getpid_wrapper(), config.CommandBrokerURI.Topic,
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  consumer = KafkaW::createConsumer(BrokerSettings);
  if (consumer->topicPresent(config.CommandBrokerURI.Topic))
    consumer->addTopic(config.CommandBrokerURI.Topic);
  else {
    Logger->error(
        "Topic {} not in broker. Could not start listener for topic {}.",
        config.CommandBrokerURI.Topic, config.CommandBrokerURI.Topic);
    throw std::runtime_error(fmt::format(
        "Topic {} not in broker. Could not start listener for topic {}.",
        config.CommandBrokerURI.Topic, config.CommandBrokerURI.Topic));
  }
}

std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> CommandListener::poll() {
  return consumer->poll();
}

} // namespace FileWriter
