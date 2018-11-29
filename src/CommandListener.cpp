#include "CommandListener.h"
#include "helper.h"
#include "logger.h"

namespace FileWriter {

using std::string;
using std::vector;

CommandListener::CommandListener(MainOpt &config) : config(config) {}

CommandListener::~CommandListener() {}

void CommandListener::start() {
  KafkaW::BrokerSettings BrokerSettings;
  BrokerSettings.PollTimeoutMS = 500;
  BrokerSettings.Address = config.command_broker_uri.host_port;
  BrokerSettings.KafkaConfiguration["group.id"] = fmt::format(
      "filewriter--commandhandler--host:{}--pid:{}--topic:{}--time:{}",
      gethostname_wrapper(), getpid_wrapper(), config.command_broker_uri.topic,
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  consumer.reset(new KafkaW::Consumer(BrokerSettings));
  consumer->on_rebalance_assign = config.on_rebalance_assign;
  consumer->addTopic(config.command_broker_uri.topic);
  if (config.start_at_command_offset >= 0) {
    int n1 = config.start_at_command_offset;
    consumer->on_rebalance_start =
        [n1](rd_kafka_topic_partition_list_t *plist) {
          for (int i1 = 0; i1 < plist->cnt; ++i1) {
            plist->elems[i1].offset = n1;
          }
        };
  }
}

std::unique_ptr<KafkaW::ConsumerMessage> CommandListener::poll() {
  return consumer->poll();
}

} // namespace FileWriter
