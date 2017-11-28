#include <algorithm>
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <iostream>
#include <stdexcept>
#include <thread>

#include "../Streamer.hpp"
#include <librdkafka/rdkafkacpp.h>

#include "../uri.h"
#include "consumer.hpp"
#include "producer.hpp"

using namespace FileWriter;

class StreamerTest : public ::testing::Test {

protected:
  using Option = Streamer::option_t;
  using Options = Streamer::Options;

  virtual void SetUp();
  std::unique_ptr<Streamer> s{nullptr};

  Streamer::SEC reset() { return s->close_stream(); }

  void set_streamer_options(const Options &opt) {
    s->set_streamer_options(opt);
  }
  const milliseconds &ms_before_start_time() { return s->ms_before_start_time; }
  const milliseconds &start_ts() { return s->start_ts; }
  const int &metadata_retry() { return s->metadata_retry; }

  std::unique_ptr<RdKafka::Conf> get_configuration(const Options &opt = {}) {
    return s->create_configuration(opt);
  }

  Streamer::SEC create_consumer(std::unique_ptr<RdKafka::Conf> &&config) {
    return s->create_consumer(std::move(config));
  }
  std::unique_ptr<RdKafka::Metadata> create_metadata() {
    return s->create_metadata();
  }
  Streamer::SEC
  create_topic_partition(const std::string &topic,
                         std::unique_ptr<RdKafka::Metadata> &&metadata) {
    return s->create_topic_partition(topic, std::move(metadata));
  }
  const std::vector<RdKafka::TopicPartition *> &get_topic_partition() {
    return s->_tp;
  }
  Streamer::SEC assign_topic_partition() { return s->assign_topic_partition(); }
};

void StreamerTest::SetUp() {
  s.reset(new Streamer{Producer::broker, Producer::topic});
  if (!s->consumer) {
    FAIL();
  } else {
    SUCCEED();
  }
}

TEST(Streamer, test_producer) {
  Producer p;
  p.SetUp();
  p.produce();
}

TEST_F(StreamerTest, streamer_initialisation) {}

TEST_F(StreamerTest, empty_option_list) {
  // verify initial values
  auto ms_before_start_time_ = ms_before_start_time();
  auto metadata_retry_ = metadata_retry();
  auto start_ts_ = start_ts();
  ASSERT_EQ(ms_before_start_time_, ms_before_start_time());
  ASSERT_EQ(metadata_retry_, metadata_retry());
  ASSERT_EQ(start_ts_, start_ts());

  set_streamer_options({{"", ""}});
  EXPECT_EQ(ms_before_start_time_, ms_before_start_time());
  EXPECT_EQ(metadata_retry_, metadata_retry());
  EXPECT_EQ(start_ts_, start_ts());
}

TEST_F(StreamerTest, set_one_streamer_option) {
  auto ms_before_start_time_ = ms_before_start_time();
  auto metadata_retry_ = metadata_retry();
  auto start_ts_ = start_ts();

  set_streamer_options({{"start-time-ms", "1234"}});

  EXPECT_EQ(ms_before_start_time_, ms_before_start_time());
  EXPECT_EQ(metadata_retry_, metadata_retry());
  EXPECT_EQ(milliseconds{1234}, start_ts());
}

TEST_F(StreamerTest, set_all_streamer_options) {
  set_streamer_options({{"start-time-ms", "1234"},
                        {"metadata-retry", "10"},
                        {"ms-before-start", "100"}});

  EXPECT_EQ(milliseconds{100}, ms_before_start_time());
  EXPECT_EQ(10, metadata_retry());
  EXPECT_EQ(milliseconds{1234}, start_ts());
}

TEST_F(StreamerTest, create_default_rdkafka_conf) {
  auto configuration = get_configuration();
  if (!configuration) {
    FAIL();
  } else {
    SUCCEED();
  }
}

TEST_F(StreamerTest, set_and_verify_rdkafka_conf_options) {
  auto configuration =
      get_configuration({{"metadata.broker.list", Producer::broker},
                         {"message.max.bytes", "10000000"}});
  if (!configuration) {
    FAIL();
  } else {
    SUCCEED();
  }
  std::string value;
  configuration->get("metadata.broker.list", value);
  EXPECT_EQ(Producer::broker, value);
  configuration->get("message.max.bytes", value);
  EXPECT_EQ("10000000", value);
}

TEST_F(StreamerTest, create_rdkafka_kafkaconsumer) {
  auto configuration =
      get_configuration({{"metadata.broker.list", Producer::broker},
                         {"message.max.bytes", "10000000"},
                         {"group.id", "streamer-test"}});
  auto err = create_consumer(std::move(configuration));
  EXPECT_EQ(Streamer::SEC::no_error, err);
}

TEST_F(StreamerTest, create_rdkafka_metadata) {
  auto configuration =
      get_configuration({{"metadata.broker.list", Producer::broker},
                         {"message.max.bytes", "10000000"},
                         {"group.id", "streamer-test"}});
  create_consumer(std::move(configuration));
  auto metadata = create_metadata();
  if (!metadata) {
    FAIL();
  } else {
    SUCCEED();
  }
}

TEST_F(StreamerTest, rdkafka_topic_partition_fails_when_metadata_is_null) {
  auto err = create_topic_partition("missing-test-topic", std::move(nullptr));
  EXPECT_EQ(Streamer::SEC::metadata_error, err);
}

TEST_F(StreamerTest,
       rdkafka_topic_partition_fails_when_partition_doesn_t_exist) {
  auto configuration =
      get_configuration({{"metadata.broker.list", Producer::broker},
                         {"message.max.bytes", "10000000"},
                         {"group.id", "streamer-test"}});
  create_consumer(std::move(configuration));
  auto metadata = create_metadata();

  auto err = create_topic_partition("missing-test-topic", std::move(metadata));
  EXPECT_EQ(Streamer::SEC::topic_partition_error, err);
}

TEST_F(StreamerTest, create_rdkafka_topic_partition) {
  // delete producer and topic_partition array
  reset();

  auto configuration =
      get_configuration({{"metadata.broker.list", Producer::broker},
                         {"group.id", "streamer-test"}});
  create_consumer(std::move(configuration));
  auto metadata = create_metadata();

  auto err = create_topic_partition(Producer::topic, std::move(metadata));
  EXPECT_EQ(Streamer::SEC::no_error, err);

  auto tp = get_topic_partition();
  EXPECT_GT(tp.size(), 0);

  std::vector<int> partition_list;
  for (auto &i : tp) {
    EXPECT_EQ(Producer::topic, i->topic());
    auto pos =
        find(partition_list.begin(), partition_list.end(), i->partition());
    EXPECT_EQ(pos, partition_list.end());
    partition_list.push_back(i->partition());
  }
}

TEST_F(StreamerTest,
       assign_rdkafka_topic_partition_fails_if_no_consumer_or_empty_tp) {
  reset();
  auto configuration =
      get_configuration({{"metadata.broker.list", Producer::broker},
                         {"group.id", "streamer-test"}});

  auto err = assign_topic_partition();
  EXPECT_EQ(Streamer::SEC::topic_partition_error, err);

  create_consumer(std::move(configuration));
  EXPECT_EQ(Streamer::SEC::topic_partition_error, err);
}

TEST_F(StreamerTest, assign_rdkafka_topic_partition_succeed) {

  auto configuration =
      get_configuration({{"metadata.broker.list", Producer::broker},
                         {"group.id", "streamer-test"}});

  auto err = assign_topic_partition();
  EXPECT_EQ(Streamer::SEC::no_error, err);

  create_consumer(std::move(configuration));
  EXPECT_EQ(Streamer::SEC::no_error, err);
}

TEST_F(StreamerTest, write) {

  DemuxTopic mp(Producer::topic);
  auto result = s->write(mp);

  EXPECT_TRUE(result.is_OK());

}

int main(int argc, char **argv) {

  ::testing::InitGoogleTest(&argc, argv);

  for (int i = 1; i < argc; ++i) {
    if (std::string{argv[i]} == "--broker" || std::string{argv[i]} == "-b") {
      uri::URI u(argv[i + 1]);
      Producer::broker = u.host_port;
      Producer::topic = u.topic;
      Consumer::broker = u.host_port;
      Consumer::topic = u.topic;
    }
    if (std::string{argv[i]} == "--help" || std::string{argv[i]} == "-h") {
      std::cout << "\nOptions: "
                << "\n"
                << "\t--broker //<host>:<port>/<topic>\n";
    }
  }

  return RUN_ALL_TESTS();
}
