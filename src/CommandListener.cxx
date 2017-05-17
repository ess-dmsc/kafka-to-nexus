#include "CommandListener.h"
#include "KafkaW.h"
#include "Master_handler.h"
#include "commandproducer.h"
#include "helper.h"
#include "kafka_util.h"
#include "logger.h"
#include <cassert>
#include <map>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;

std::unique_ptr<CmdMsg> PollStatus::is_CmdMsg() {
  if (state == 1) {
    std::unique_ptr<CmdMsg> ret((CmdMsg *)data);
    data = nullptr;
    return ret;
  }
  return nullptr;
}

CommandListener::CommandListener(CommandListenerConfig config)
    : config(config) {}

CommandListener::~CommandListener() {}

void CommandListener::start() {
  KafkaW::BrokerOpt opt;
  opt.poll_timeout_ms = 500;
  opt.address = config.broker.host_port;
  opt.conf_strings["group.id"] =
      fmt::format("kafka-to-nexus.CommandListener--pid-{}", getpid());
  consumer.reset(new KafkaW::Consumer(opt));
  consumer->on_rebalance_assign = config.on_rebalance_assign;
  consumer->add_topic(config.broker.topic);
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

KafkaW::PollStatus CommandListener::poll() { return consumer->poll(); }

} // namespace FileWriter
} // namespace BrightnESS
