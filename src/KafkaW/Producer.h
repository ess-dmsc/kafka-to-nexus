#pragma once

#include "BrokerSettings.h"
#include "ConsumerMessage.h"
#include <atomic>
#include <functional>
#include <librdkafka/rdkafka.h>

namespace KafkaW {

class ProducerTopic;

class ProducerMsg {
public:
  virtual ~ProducerMsg() = default;
  uchar *data;
  uint32_t size;
};

struct ProducerStats {
  std::atomic<uint64_t> produced{0};
  std::atomic<uint32_t> produce_fail{0};
  std::atomic<uint32_t> local_queue_full{0};
  std::atomic<uint64_t> produce_cb{0};
  std::atomic<uint64_t> produce_cb_fail{0};
  std::atomic<uint64_t> poll_served{0};
  std::atomic<uint64_t> msg_too_large{0};
  std::atomic<uint64_t> produced_bytes{0};
  std::atomic<uint32_t> out_queue{0};
  ProducerStats() = default;
  ProducerStats(ProducerStats const &) = default;
};

class ProducerInterface {
public:
  ProducerInterface() = default;
  virtual ~ProducerInterface() = default;
  virtual void poll() = 0;
  virtual uint64_t outputQueueLength() = 0;
};

class Producer : public ProducerInterface {
public:
  typedef ProducerTopic Topic;
  typedef ProducerMsg Msg;
  explicit Producer(BrokerSettings const &Settings);
  explicit Producer(Producer const &) = delete;
  explicit Producer(Producer &&x);
  ~Producer() override;

  void poll() override;

  uint64_t outputQueueLength() override;
  static void cb_delivered(rd_kafka_t *RK, rd_kafka_message_t const *Message,
                           void *Opaque);
  static void cb_error(rd_kafka_t *RK, int ErrorCode, char const *ErrorMessage,
                       void *Opaque);
  static int cb_stats(rd_kafka_t *RK, char *JSON, size_t JSONLength,
                      void *Opaque);
  static void cb_log(rd_kafka_t const *RK, int Level, char const *Fac,
                     char const *Buf);
  static void cb_throttle(rd_kafka_t *RK, char const *BrokerName,
                          int32_t BrokerID, int ThrottleTime_ms, void *Opaque);
  rd_kafka_t *getRdKafkaPtr() const;
  std::function<void(rd_kafka_message_t const *msg)> on_delivery_ok;
  std::function<void(rd_kafka_message_t const *msg)> on_delivery_failed;
  std::function<void(Producer *, rd_kafka_resp_err_t)> on_error;
  // Currently it's nice to have access to these two for statistics:
  BrokerSettings ProducerBrokerSettings;
  rd_kafka_t *RdKafkaPtr = nullptr;
  std::atomic<uint64_t> TotalMessagesProduced{0};
  ProducerStats Stats;

private:
  int id = 0;
};
}
