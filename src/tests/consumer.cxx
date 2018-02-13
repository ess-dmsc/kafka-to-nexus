#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>

#include "consumer.hpp"

/////////////
/////////////
std::string Consumer::broker = "localhost";
std::string Consumer::topic = "test";
uint64_t Consumer::timestamp = 0;

void Consumer::create_config() {
  conf.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (conf) {
    SUCCEED();
  } else {
    FAIL();
  }
  std::string errstr;
  if (conf->set("metadata.broker.list", broker, errstr) !=
      RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << "\n";
    FAIL();
  }
  if (conf->set("api.version.request", "true", errstr) !=
      RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << "\n";
    FAIL();
  }
  if (conf->set("group.id", "0", errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << "\n";
    FAIL();
  }
};

void Consumer::create_consumer() {
  std::string errstr;
  if (!conf) {
    FAIL();
  }
  //  auto c = RdKafka::KafkaConsumer::create(conf.get(), errstr);
  consumer.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
  if (!consumer) {
    std::cerr << errstr << "\n";
    FAIL();
  }
};

void Consumer::offsets_for_times() {
  if (!consumer || tp.empty()) {
    FAIL();
  }
  auto err = consumer->offsetsForTimes(tp, 1000);
  if (err != RdKafka::ERR_NO_ERROR) {
    std::cerr << RdKafka::err2str(err) << "\n";
    FAIL();
  } else {
    SUCCEED();
  }
};

void Consumer::create_metadata() {
  RdKafka::Metadata *md;
  std::unique_ptr<RdKafka::Topic> ptopic;
  auto err = consumer->metadata(ptopic.get() != NULL, ptopic.get(), &md, 1000);
  if (err != RdKafka::ERR_NO_ERROR) {
    std::cerr << RdKafka::err2str(err) << "\n";
    FAIL();
  } else {
    metadata.reset(md);
    SUCCEED();
  }
}

void Consumer::push_topic_partition(const unsigned &partition) {
  if (timestamp) {
    tp.push_back(RdKafka::TopicPartition::create(topic, partition, timestamp));
  } else {
    tp.push_back(RdKafka::TopicPartition::create(topic, partition));
  }
}

void Consumer::create_topic_partition() {
  using PartitionMetadataVector =
      std::vector<const RdKafka::PartitionMetadata *>;
  if (metadata) {
    const PartitionMetadataVector *pmv;
    for (auto &t : *metadata->topics()) {
      if (t->topic() == topic) {
        pmv = t->partitions();
        break;
      }
    }
    if (pmv->size()) {
      for (auto p : *pmv) {
        push_topic_partition(p->id());
        if (!tp.back()) {
          FAIL();
        }
      }
    } else {
      push_topic_partition(0);
      if (!tp.back()) {
        FAIL();
      }
    }
  }
};

void Consumer::assign_topic_partition() {
  if (!consumer || tp.empty()) {
    FAIL();
  }
  auto err = consumer->assign(tp);
  if (err != RdKafka::ERR_NO_ERROR) {
    std::cerr << RdKafka::err2str(err) << "\n";
    FAIL();
  } else {
    SUCCEED();
  }
};

void Consumer::consume() {
  if (!consumer) {
    FAIL();
  }
  std::unique_ptr<RdKafka::Message> msg{consumer->consume(100)};
  if (msg->err() == RdKafka::ERR__PARTITION_EOF ||
      msg->err() == RdKafka::ERR__TIMED_OUT) {
    std::cerr << "consume :\t" << RdKafka::err2str(msg->err()) << "\n";
  }
  if (msg->err() == RdKafka::ERR_NO_ERROR) {
    std::cout << std::string((char *)msg->payload()) << "\t"
              << msg->timestamp().timestamp << "\n";
    ;
  }
}
