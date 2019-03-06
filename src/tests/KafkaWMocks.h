#pragma once
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
  MockKafkaConsumer(){};
  MockKafkaConsumer(RdKafka::ErrorCode ErrorCode, RdKafka::Metadata *Metadata)
      : ErrorCode(ErrorCode), MetadataPtr(Metadata) {}
  RdKafka::ErrorCode ErrorCode;
  RdKafka::Metadata *MetadataPtr;

  RdKafka::ErrorCode metadata(bool, const RdKafka::Topic *,
                              RdKafka::Metadata **Metadata, int) override {

    *Metadata = MetadataPtr;
    return ErrorCode;
  }

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
};

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
