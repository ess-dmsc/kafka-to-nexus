#include <Status.hpp>

#include <gtest/gtest.h>

#include <random>
#include <thread>

using namespace FileWriter;
const int n_messages = 10000;
const int error_ratio = 20;

std::default_random_engine generator;
std::normal_distribution<double> normal(0.0, 1.0);

const double bernoulli_avg = 0.5;
std::bernoulli_distribution bernoulli(bernoulli_avg);
std::bernoulli_distribution b(10);

void collect(Status::StreamerStatus &s, double &x) {
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

void continous_collect(Status::StreamerStatus &s, bool &terminate) {
  int i{0};
  while (!terminate) {
    // std::this_thread::sleep_for(std::chrono::milliseconds(10));
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

void fetch(Status::StreamerStatus &s, double &x) {
  Status::StreamerStatusType v = std::move(s.fetch_status());
  EXPECT_EQ(v.messages, n_messages - n_messages / error_ratio);
  EXPECT_EQ(v.bytes, x);
  EXPECT_EQ(v.errors, n_messages / error_ratio);
}

void continous_fetch(Status::StreamerStatus &s, bool &terminate) {
  Status::StreamerStatusType old;
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

void continous_statistics(Status::StreamerStatus &s, bool &terminate) {

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  while (!terminate) {
    auto v = s.fetch_statistics();
    // with 5\sigma we expect >99.99..% of evts ok
    EXPECT_TRUE(v.size_avg - 5 * v.size_std < bernoulli_avg);
    EXPECT_TRUE(v.size_avg + 5 * v.size_std > bernoulli_avg);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}

TEST(StreamerStatus, Counters) {
  Status::StreamerStatus s_;
  double x{0};
  std::thread update(collect, std::ref(s_), std::ref(x));
  update.join();
  std::thread test(fetch, std::ref(s_), std::ref(x));
  test.join();
}

TEST(StreamerStatus, ContinousCounters) {
  Status::StreamerStatus s_;
  bool stop = false;
  std::thread update(continous_collect, std::ref(s_), std::ref(stop));
  std::thread test(continous_fetch, std::ref(s_), std::ref(stop));
  std::this_thread::sleep_for(std::chrono::seconds(2));
  stop = true;
  update.join();
  test.join();
}

TEST(StreamerStatus, ContinousStatistics) {
  Status::StreamerStatus s_;
  bool stop = false;
  std::thread update(continous_collect, std::ref(s_), std::ref(stop));
  std::thread test(continous_fetch, std::ref(s_), std::ref(stop));
  std::thread stat(continous_statistics, std::ref(s_), std::ref(stop));
  std::this_thread::sleep_for(std::chrono::seconds(5));
  stop = true;
  update.join();
  test.join();
  stat.join();
}

int main(int argc, char **argv) {

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
