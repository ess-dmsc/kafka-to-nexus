// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include "Kafka/ProducerTopic.h"
#include <librdkafka/rdkafkacpp.h>
#include <trompeloeil.hpp>

class MockMetadata : public RdKafka::Metadata {
public:
  MAKE_CONST_MOCK0(brokers, const RdKafka::Metadata::BrokerMetadataVector *());
  MAKE_CONST_MOCK0(topics, const RdKafka::Metadata::TopicMetadataVector *());
  MAKE_CONST_MOCK0(orig_broker_id, int32_t());
  MAKE_CONST_MOCK0(orig_broker_name, const std::string());
};

class MockTopicMetadata : public RdKafka::TopicMetadata {
private:
  std::string Name;

public:
  explicit MockTopicMetadata(const std::string &TopicName) : Name(TopicName) {}

  const std::string topic() const override { return Name; }

  MAKE_CONST_MOCK0(partitions, const PartitionMetadataVector *(), override);
  MAKE_CONST_MOCK0(err, RdKafka::ErrorCode(), override);
};

class MockPartitionMetadata : public RdKafka::PartitionMetadata {
public:
  MAKE_CONST_MOCK0(id, int32_t());
  MAKE_CONST_MOCK0(err, RdKafka::ErrorCode());
  MAKE_CONST_MOCK0(leader, int32_t());
  MAKE_CONST_MOCK0(replicas, const std::vector<int32_t> *());
  MAKE_CONST_MOCK0(isrs, const std::vector<int32_t> *());
};

class MockKafkaConsumer : public RdKafka::KafkaConsumer {
public:
  MockKafkaConsumer() = default;
  MockKafkaConsumer(RdKafka::ErrorCode ErrorCode, RdKafka::Metadata *Metadata)
      : ErrorCode(ErrorCode), MetadataPtr(Metadata) {}
  RdKafka::ErrorCode ErrorCode = RdKafka::ErrorCode::ERR_NO_ERROR;
  RdKafka::Metadata *MetadataPtr = nullptr;

  // Kafka::Consumer may be calling this method multiple times in case
  // RdKafka::ERR_TRANSPORT is returned. After few unsuccessful calls
  // ERR_NO_ERROR is returned to simulate established connection.
  RdKafka::ErrorCode metadata(bool, const RdKafka::Topic *,
                              RdKafka::Metadata **Metadata, int) override {
    *Metadata = MetadataPtr;
    metadataCallCounter++;
    if (metadataCallCounter < 6) {
      return ErrorCode;
    } else {
      return RdKafka::ERR_NO_ERROR;
    }
  }

public:
  MAKE_CONST_MOCK0(name, const std::string(), override);
  MAKE_CONST_MOCK0(memberid, const std::string(), override);
  MAKE_MOCK1(poll, int(int), override);
  MAKE_MOCK0(outq_len, int(), override);
  MAKE_MOCK1(pause,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK1(resume,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK5(query_watermark_offsets,
             RdKafka::ErrorCode(const std::string &, int32_t, int64_t *,
                                int64_t *, int),
             override);
  MAKE_MOCK4(get_watermark_offsets,
             RdKafka::ErrorCode(const std::string &, int32_t, int64_t *,
                                int64_t *),
             override);
  MAKE_MOCK2(offsetsForTimes,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &, int),
             override);
  MAKE_MOCK1(get_partition_queue,
             RdKafka::Queue *(const RdKafka::TopicPartition *), override);
  MAKE_MOCK1(set_log_queue, RdKafka::ErrorCode(RdKafka::Queue *), override);
  MAKE_MOCK0(yield, void(), override);
  MAKE_MOCK1(clusterid, const std::string(int), override);
  MAKE_MOCK0(c_ptr, rd_kafka_s *(), override);
  MAKE_MOCK1(subscribe, RdKafka::ErrorCode(const std::vector<std::string> &),
             override);
  MAKE_MOCK0(unsubscribe, RdKafka::ErrorCode(), override);
  MAKE_MOCK1(assign,
             RdKafka::ErrorCode(const std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK0(unassign, RdKafka::ErrorCode(), override);
  MAKE_MOCK1(consume, RdKafka::Message *(int), override);
  MAKE_MOCK0(commitSync, RdKafka::ErrorCode(), override);
  MAKE_MOCK0(commitAsync, RdKafka::ErrorCode(), override);
  MAKE_MOCK1(commitSync, RdKafka::ErrorCode(RdKafka::Message *), override);
  MAKE_MOCK1(commitAsync, RdKafka::ErrorCode(RdKafka::Message *), override);
  MAKE_MOCK1(commitSync,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK1(commitAsync,
             RdKafka::ErrorCode(const std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK1(commitSync, RdKafka::ErrorCode(RdKafka::OffsetCommitCb *),
             override);
  MAKE_MOCK2(commitSync,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &,
                                RdKafka::OffsetCommitCb *),
             override);
  MAKE_MOCK2(committed,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &, int),
             override);
  MAKE_MOCK1(position,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK0(close, RdKafka::ErrorCode(), override);
  MAKE_MOCK2(seek, RdKafka::ErrorCode(const RdKafka::TopicPartition &, int),
             override);
  MAKE_MOCK1(offsets_store,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK1(assignment,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK1(subscription, RdKafka::ErrorCode(std::vector<std::string> &),
             override);
  MAKE_MOCK1(controllerid, int32_t(int), override);
  MAKE_MOCK1(fatal_error, RdKafka::ErrorCode(std::string &), override);
  MAKE_MOCK5(oauthbearer_set_token,
             RdKafka::ErrorCode(const std::string &, int64_t,
                                const std::string &,
                                const std::list<std::string> &, std::string &),
             override);
  MAKE_MOCK1(oauthbearer_set_token_failure,
             RdKafka::ErrorCode(const std::string &), override);

private:
  int metadataCallCounter = 0;
};

class MockProducer : public RdKafka::Producer {
public:
  MAKE_CONST_MOCK0(name, const std::string(), override);
  MAKE_CONST_MOCK0(memberid, const std::string(), override);
  MAKE_MOCK1(poll, int(int), override);
  MAKE_MOCK0(outq_len, int(), override);
  MAKE_MOCK4(metadata,
             RdKafka::ErrorCode(bool, const RdKafka::Topic *,
                                RdKafka::Metadata **, int),
             override);
  MAKE_MOCK1(pause,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK1(resume,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK5(query_watermark_offsets,
             RdKafka::ErrorCode(const std::string &, int32_t, int64_t *,
                                int64_t *, int),
             override);
  MAKE_MOCK4(get_watermark_offsets,
             RdKafka::ErrorCode(const std::string &, int32_t, int64_t *,
                                int64_t *),
             override);
  MAKE_MOCK2(offsetsForTimes,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &, int),
             override);
  MAKE_MOCK1(get_partition_queue,
             RdKafka::Queue *(const RdKafka::TopicPartition *), override);
  MAKE_MOCK1(set_log_queue, RdKafka::ErrorCode(RdKafka::Queue *), override);
  MAKE_MOCK0(yield, void(), override);
  MAKE_MOCK1(clusterid, const std::string(int), override);
  MAKE_MOCK0(c_ptr, rd_kafka_s *(), override);
  MAKE_MOCK2(create, RdKafka::Producer *(RdKafka::Conf *, std::string));
  MAKE_MOCK7(produce,
             RdKafka::ErrorCode(RdKafka::Topic *, int32_t, int, void *, size_t,
                                const std::string *, void *),
             override);
  MAKE_MOCK8(produce,
             RdKafka::ErrorCode(RdKafka::Topic *, int32_t, int, void *, size_t,
                                const void *, size_t, void *),
             override);
  MAKE_MOCK9(produce,
             RdKafka::ErrorCode(const std::string, int32_t, int, void *, size_t,
                                const void *, size_t, int64_t, void *),
             override);
  MAKE_MOCK5(produce,
             RdKafka::ErrorCode(RdKafka::Topic *, int32_t,
                                const std::vector<char> *,
                                const std::vector<char> *, void *),
             override);
  MAKE_MOCK1(flush, RdKafka::ErrorCode(int), override);
  MAKE_MOCK1(controllerid, int32_t(int), override);
  MAKE_MOCK1(fatal_error, RdKafka::ErrorCode(std::string &), override);
  MAKE_MOCK5(oauthbearer_set_token,
             RdKafka::ErrorCode(const std::string &, int64_t,
                                const std::string &,
                                const std::list<std::string> &, std::string &),
             override);
  MAKE_MOCK1(oauthbearer_set_token_failure,
             RdKafka::ErrorCode(const std::string &), override);
  MAKE_MOCK10(produce,
              RdKafka::ErrorCode(std::string, int32_t, int, void *, size_t,
                                 const void *, size_t, int64_t,
                                 RdKafka::Headers *, void *),
              override);
  MAKE_MOCK1(purge, RdKafka::ErrorCode(int), override);
};
