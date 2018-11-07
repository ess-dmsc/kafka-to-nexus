#include <chrono>

#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>

#include "../utils.h"
#include "producer.hpp"

std::string Producer::broker = "localhost";
std::string Producer::topic = "test";

void Producer::SetUp() {
  create_config();
  create_producer();
}

void Producer::create_config() {
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

void Producer::create_producer() {
  std::string errstr;
  producer.reset(RdKafka::Producer::create(conf.get(), errstr));
  if (producer) {
    SUCCEED();
  } else {
    std::cerr << errstr << "\n";
    FAIL();
  }
}

void Producer::produce(const std::string &topic, const int32_t &partition,
                       char *message) {
  using namespace std::chrono;
  auto ts = duration_cast<FileWriter::KafkaTimeStamp>(
      system_clock::now().time_since_epoch());
  auto msg = "test message < " + std::to_string(ts.count());
  if (message) {
    msg = std::string(message);
  }
  auto resp = producer->produce(
      topic, partition, RdKafka::Producer::RK_MSG_COPY,
      const_cast<char *>(msg.c_str()), msg.size(), NULL, 0, ts.count(), NULL);
  ASSERT_EQ(resp, RdKafka::ERR_NO_ERROR);
  resp = producer->flush(-1);
  ASSERT_EQ(resp, RdKafka::ERR_NO_ERROR);
}

int Producer::poll(const int &ms) { return producer->poll(ms); }
