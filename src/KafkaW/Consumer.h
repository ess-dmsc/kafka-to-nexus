#pragma once

#include "BrokerSettings.h"
#include "Msg.h"
#include "PollStatus.h"
#include <librdkafka/rdkafka.h>
//#include <functional>

namespace KafkaW {

class Inspect;

class Consumer {
public:
  Consumer(BrokerSettings opt);
  Consumer(Consumer &&) = delete;
  Consumer(Consumer const &) = delete;
  ~Consumer();
  void init();
  void addTopic(std::string Topic);
  void dump_current_subscription();
  PollStatus poll();
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
      on_rebalance_assign;
  std::function<void(rd_kafka_topic_partition_list_t *plist)>
      on_rebalance_start;
  rd_kafka_t *rk = nullptr;

private:
  BrokerSettings ConsumerBrokerSettings;
  static void cb_log(rd_kafka_t const *rk, int level, char const *fac,
                     char const *buf);
  static int cb_stats(rd_kafka_t *rk, char *json, size_t json_size,
                      void *opaque);
  static void cb_error(rd_kafka_t *rk, int err_i, char const *reason,
                       void *opaque);
  static void cb_rebalance(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                           rd_kafka_topic_partition_list_t *plist,
                           void *opaque);
  static void cb_consume(rd_kafka_message_t *msg, void *opaque);
  rd_kafka_topic_partition_list_t *plist = nullptr;
  int id = 0;
};
}
