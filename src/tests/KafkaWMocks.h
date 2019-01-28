#pragma once
#include "KafkaW/KafkaW.h"
#include <gmock/gmock.h>
#include <librdkafka/rdkafkacpp.h>
class MockMetadata : public RdKafka::Metadata {
public:
  MOCK_CONST_METHOD0(brokers,
                     const RdKafka::Metadata::BrokerMetadataVector *());
  MOCK_CONST_METHOD0(topics, const RdKafka::Metadata::TopicMetadataVector *());
  MOCK_CONST_METHOD0(orig_broker_id, int32_t());
  MOCK_CONST_METHOD0(orig_broker_name, const std::string());
};

class MockTopicMetadata : public RdKafka::TopicMetadata {
private:
  std::string Name;

public:
  explicit MockTopicMetadata(const std::string &TopicName) : Name(TopicName) {}

  const std::string topic() const override { return Name; }

  MOCK_CONST_METHOD0(partitions, PartitionMetadataVector *());
  MOCK_CONST_METHOD0(err, RdKafka::ErrorCode());
};

class MockPartitionMetadata : public RdKafka::PartitionMetadata {
public:
  MOCK_CONST_METHOD0(id, int32_t());
  MOCK_CONST_METHOD0(err, RdKafka::ErrorCode());
  MOCK_CONST_METHOD0(leader, int32_t());
  MOCK_CONST_METHOD0(replicas, const std::vector<int32_t> *());
  MOCK_CONST_METHOD0(isrs, const std::vector<int32_t> *());
};

class MockKafkaConsumer : public RdKafka::KafkaConsumer {
public:
  MOCK_CONST_METHOD0(name, const std::string());
  MOCK_CONST_METHOD0(memberid, const std::string());
  MOCK_METHOD1(poll, int(int));
  MOCK_METHOD0(outq_len, int());
  MOCK_METHOD4(metadata, RdKafka::ErrorCode(bool, const RdKafka::Topic *,
                                            RdKafka::Metadata **, int));
  MOCK_METHOD1(pause,
               RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &));
  MOCK_METHOD1(resume,
               RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &));
  MOCK_METHOD5(query_watermark_offsets,
               RdKafka::ErrorCode(const std::string &, int32_t, int64_t *,
                                  int64_t *, int));
  MOCK_METHOD4(get_watermark_offsets,
               RdKafka::ErrorCode(const std::string &, int32_t, int64_t *,
                                  int64_t *));
  MOCK_METHOD2(offsetsForTimes,
               RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &,
                                  int));
  MOCK_METHOD1(get_partition_queue,
               RdKafka::Queue *(const RdKafka::TopicPartition *));
  MOCK_METHOD1(set_log_queue, RdKafka::ErrorCode(RdKafka::Queue *));
  MOCK_METHOD0(yield, void());
  MOCK_METHOD1(clusterid, const std::string(int));
  MOCK_METHOD0(c_ptr, rd_kafka_s *());
  MOCK_METHOD1(subscribe, RdKafka::ErrorCode(const std::vector<std::string> &));
  MOCK_METHOD0(unsubscribe, RdKafka::ErrorCode());
  MOCK_METHOD1(assign, RdKafka::ErrorCode(
                           const std::vector<RdKafka::TopicPartition *> &));
  MOCK_METHOD0(unassign, RdKafka::ErrorCode());
  MOCK_METHOD1(consume, RdKafka::Message *(int));
  MOCK_METHOD0(commitSync, RdKafka::ErrorCode());
  MOCK_METHOD0(commitAsync, RdKafka::ErrorCode());
  MOCK_METHOD1(commitSync, RdKafka::ErrorCode(RdKafka::Message *));
  MOCK_METHOD1(commitAsync, RdKafka::ErrorCode(RdKafka::Message *));
  MOCK_METHOD1(commitSync,
               RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &));
  MOCK_METHOD1(
      commitAsync,
      RdKafka::ErrorCode(const std::vector<RdKafka::TopicPartition *> &));
  MOCK_METHOD1(commitSync, RdKafka::ErrorCode(RdKafka::OffsetCommitCb *));
  MOCK_METHOD2(commitSync,
               RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &,
                                  RdKafka::OffsetCommitCb *));
  MOCK_METHOD2(committed,
               RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &,
                                  int));
  MOCK_METHOD1(position,
               RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &));
  MOCK_METHOD0(close, RdKafka::ErrorCode());
  MOCK_METHOD2(seek, RdKafka::ErrorCode(const RdKafka::TopicPartition &, int));
  MOCK_METHOD1(offsets_store,
               RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &));
  MOCK_METHOD1(assignment,
               RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &));
  MOCK_METHOD1(subscription, RdKafka::ErrorCode(std::vector<std::string> &));
  MOCK_METHOD1(controllerid, int32_t(int));
};

class MockMessage : public RdKafka::Message {
public:
  MOCK_CONST_METHOD0(errstr, std::string());
  MOCK_CONST_METHOD0(err, RdKafka::ErrorCode());
  MOCK_CONST_METHOD0(topic, RdKafka::Topic *());
  MOCK_CONST_METHOD0(topic_name, std::string());
  MOCK_CONST_METHOD0(partition, int32_t());
  MOCK_CONST_METHOD0(payload, void *());
  MOCK_CONST_METHOD0(len, size_t());
  MOCK_CONST_METHOD0(key, const std::string *());
  MOCK_CONST_METHOD0(key_pointer, const void *());
  MOCK_CONST_METHOD0(key_len, size_t());
  MOCK_CONST_METHOD0(offset, int64_t());
  MOCK_CONST_METHOD0(timestamp, RdKafka::MessageTimestamp());
  MOCK_CONST_METHOD0(msg_opaque, void *());
  MOCK_CONST_METHOD0(latency, int64_t());
  MOCK_METHOD0(c_ptr, rd_kafka_message_s *());
};
