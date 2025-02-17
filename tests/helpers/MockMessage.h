// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include <librdkafka/rdkafkacpp.h>
#include <trompeloeil.hpp>

class MockMessage : public RdKafka::Message {
public:
  MAKE_CONST_MOCK0(errstr, std::string(), override);
  MAKE_CONST_MOCK0(err, RdKafka::ErrorCode(), override);
  MAKE_CONST_MOCK0(topic, RdKafka::Topic *(), override);
  MAKE_CONST_MOCK0(topic_name, std::string(), override);
  MAKE_CONST_MOCK0(partition, int32_t(), override);
  MAKE_CONST_MOCK0(payload, void *(), override);
  MAKE_CONST_MOCK0(len, size_t(), override);
  MAKE_CONST_MOCK0(key, const std::string *(), override);
  MAKE_CONST_MOCK0(key_pointer, const void *(), override);
  MAKE_CONST_MOCK0(key_len, size_t(), override);
  MAKE_CONST_MOCK0(offset, int64_t(), override);
  MAKE_CONST_MOCK0(timestamp, RdKafka::MessageTimestamp(), override);
  MAKE_CONST_MOCK0(msg_opaque, void *(), override);
  MAKE_CONST_MOCK0(latency, int64_t(), override);
  MAKE_CONST_MOCK0(status, RdKafka::Message::Status(), override);
  MAKE_CONST_MOCK0(broker_id, int32_t(), override);
  MAKE_CONST_MOCK0(leader_epoch, int32_t(), override);
  MAKE_MOCK0(headers, RdKafka::Headers *(), override);
  MAKE_MOCK1(headers, RdKafka::Headers *(RdKafka::ErrorCode *), override);
  MAKE_MOCK0(c_ptr, rd_kafka_message_s *(), override);
  MAKE_MOCK0(offset_store, RdKafka::Error *(), override);
};
