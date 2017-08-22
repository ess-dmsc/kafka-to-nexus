#include <thread>
#include <random>

#include <gtest/gtest.h>

#include <Status.hpp>

using StreamerStatusType = FileWriter::Status::StreamerStatusType;
using StreamerStatisticsType = FileWriter::Status::StreamerStatisticsType;
using StreamerStatus = FileWriter::Status::StreamerStatus;
using StreamMasterStatus = FileWriter::Status::StreamMasterStatus;
using StreamMasterErrorCode = FileWriter::Status::StreamMasterErrorCode;

const int n_messages = 10000;
const int error_ratio = 20;

std::default_random_engine generator;
std::normal_distribution<double> normal(0.0, 1.0);

const double bernoulli_avg = 0.5;
std::bernoulli_distribution bernoulli(bernoulli_avg);
std::bernoulli_distribution b(10);

void collect(StreamerStatus &s, double &x) {
  for (int i = 0; i < n_messages; ++i) {
    if (i % error_ratio == 0) {
      s.error();
      continue;
    }
    double n = normal(generator);
    s.add_message(n);
    x += n;
  }
  return;
}

void continuous_collect(StreamerStatus &s, std::atomic<bool> &terminate) {
  int i{0};
  while (!terminate) {
    std::this_thread::sleep_for(std::chrono::milliseconds(b(generator)));
    ++i;
    if (i % error_ratio == 0) {
      s.error();
      continue;
    }
    double n = bernoulli(generator);
    s.add_message(n);
  }
  return;
}

void fetch(StreamerStatus &s, double &x) {
  StreamerStatusType v = std::move(s.fetch_status());
  EXPECT_EQ(v.messages, n_messages - n_messages / error_ratio);
  EXPECT_EQ(v.bytes, x);
  EXPECT_EQ(v.errors, n_messages / error_ratio);
}

void continuous_fetch(StreamerStatus &s, std::atomic<bool> &terminate) {
  StreamerStatusType old;
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  while (!terminate) {
    auto v = s.fetch_status();
    EXPECT_TRUE(old.messages <= v.messages);
    EXPECT_TRUE(old.bytes <= v.bytes);
    EXPECT_TRUE(old.errors <= v.errors);
    old = v;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST(StreamerStatusType, is_empty_at_construction) {
  StreamerStatusType sst;
  ASSERT_EQ(sst.bytes, 0.0);
  ASSERT_EQ(sst.messages, 0.0);
  ASSERT_EQ(sst.errors, 0.0);
  ASSERT_EQ(sst.bytes_squared, 0.0);
  ASSERT_EQ(sst.messages_squared, 0.0);
}

TEST(StreamerStatusType, add_one_message) {
  StreamerStatusType sst, other;
  other.bytes = 1234;
  other.messages = 10;
  other.errors = 1;
  other.bytes_squared = 1234 * 180;
  other.messages_squared = 1234 * sqrt(180);

  sst += other;

  ASSERT_EQ(sst.bytes, other.bytes);
  ASSERT_EQ(sst.messages, other.messages);
  ASSERT_EQ(sst.errors, other.errors);
  ASSERT_EQ(sst.bytes_squared, other.bytes_squared);
  ASSERT_EQ(sst.messages_squared, other.messages_squared);
}

TEST(StreamerStatusType, reset_counters) {
  StreamerStatusType sst;
  sst.bytes = 1234;
  sst.messages = 10;
  sst.errors = 1;
  sst.bytes_squared = 1234 * 180;
  sst.messages_squared = 1234 * sqrt(180);
  sst.reset();

  ASSERT_EQ(sst.bytes, 0.0);
  ASSERT_EQ(sst.messages, 0.0);
  ASSERT_EQ(sst.errors, 0.0);
  ASSERT_EQ(sst.bytes_squared, 0.0);
  ASSERT_EQ(sst.messages_squared, 0.0);
}

TEST(StreamerStatus, collect_statistics_sincronously_once) {
  StreamerStatus s;
  double x{0};
  collect(s, x);
  {
    StreamerStatusType v = std::move(s.fetch_status());
    EXPECT_EQ(v.messages, n_messages - n_messages / error_ratio);
    EXPECT_EQ(v.bytes, x);
    EXPECT_EQ(v.errors, n_messages / error_ratio);
  }
}

TEST(StreamerStatus, collect_statistics_asincronously_once) {
  StreamerStatus s_;
  double x{0};
  std::thread update(collect, std::ref(s_), std::ref(x));
  update.join();
  std::thread test(fetch, std::ref(s_), std::ref(x));
  test.join();
}

TEST(StreamerStatus, collect_statistics_during_data_production) {
  StreamerStatus s;
  std::atomic<bool> stop{false};
  std::thread update(continuous_collect, std::ref(s), std::ref(stop));
  std::thread test(continuous_fetch, std::ref(s), std::ref(stop));
  std::this_thread::sleep_for(std::chrono::seconds(2));
  stop = true;
  update.join();
  test.join();
}

TEST(StreamMasterStatus, add_one_new_topic) {
  StreamMasterStatus sm;
  StreamerStatusType status;
  StreamerStatisticsType statistics;
  sm.push("topic_name", status, statistics);
  ASSERT_EQ(sm.size(), 1);
}

TEST(StreamMasterStatus, add_more_new_topics) {
  StreamMasterStatus sm;
  StreamerStatusType status;
  StreamerStatisticsType statistics;
  sm.push("topic_name", status, statistics);
  sm.push("new_topic_name", status, statistics);
  ASSERT_EQ(sm.size(), 2);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
