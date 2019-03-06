#pragma once
#include <librdkafka/rdkafkacpp.h>
#include <trompeloeil.hpp>

class MockMessage : public RdKafka::Message {
public:
  MAKE_CONST_MOCK0(errstr, std::string());
  MAKE_CONST_MOCK0(err, RdKafka::ErrorCode());
  MAKE_CONST_MOCK0(topic, RdKafka::Topic *());
  MAKE_CONST_MOCK0(topic_name, std::string());
  MAKE_CONST_MOCK0(partition, int32_t());
  MAKE_CONST_MOCK0(payload, void *());
  MAKE_CONST_MOCK0(len, size_t());
  MAKE_CONST_MOCK0(key, const std::string *());
  MAKE_CONST_MOCK0(key_pointer, const void *());
  MAKE_CONST_MOCK0(key_len, size_t());
  MAKE_CONST_MOCK0(offset, int64_t());
  MAKE_CONST_MOCK0(timestamp, RdKafka::MessageTimestamp());
  MAKE_CONST_MOCK0(msg_opaque, void *());
  MAKE_CONST_MOCK0(latency, int64_t());
  MAKE_MOCK0(c_ptr, rd_kafka_message_s *());
};
