#include "Metrics/Reporter.h"
#include "MockSink.h"
#include <chrono>
#include <gtest/gtest.h>

using namespace std::chrono_literals;

class MetricsReporterTest : public ::testing::Test {};

namespace Metrics {

TEST(MetricsReporterTest,
     MetricSuccessfullyAddedCanBeRemovedUsingSameFullName) {
  Metric TestMetric("some_name", "Description", Severity::INFO);
  auto TestSink = std::unique_ptr<Sink>(new MockSink());
  Reporter TestReporter(std::move(TestSink), 10ms);

  std::string const FullName = "some_prefix.some_name";
  ASSERT_TRUE(TestReporter.addMetric(TestMetric, FullName));
  ASSERT_TRUE(TestReporter.tryRemoveMetric(FullName));
}

TEST(MetricsReporterTest, TryingToAddMetricWithSameFullNameTwiceFails) {
  Metric TestMetric1("some_name", "Description", Severity::INFO);
  Metric TestMetric2("some_name", "Different description", Severity::DEBUG);
  auto TestSink = std::unique_ptr<Sink>(new MockSink());
  Reporter TestReporter(std::move(TestSink), 10ms);

  std::string const FullName = "some_prefix.some_name";
  ASSERT_TRUE(TestReporter.addMetric(TestMetric1, FullName));
  ASSERT_FALSE(TestReporter.addMetric(TestMetric2, FullName));
}

TEST(MetricsReporterTest,
     TryingToRemoveMetricWhichWasNotAddedFails) {
  auto TestSink = std::unique_ptr<Sink>(new MockSink());
  Reporter TestReporter(std::move(TestSink), 10ms);

  std::string const FullName = "some_prefix.some_name";
  ASSERT_FALSE(TestReporter.tryRemoveMetric(FullName));
}

} // namespace Metrics
