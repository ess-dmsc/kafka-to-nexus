#include "Metrics/Reporter.h"
#include "MockSink.h"
#include <chrono>
#include <gtest/gtest.h>

using namespace std::chrono_literals;
using trompeloeil::_;

class MetricsReporterTest : public ::testing::Test {};

namespace Metrics {

TEST(MetricsReporterTest,
     MetricSuccessfullyAddedCanBeRemovedUsingSameFullName) {
  Metric TestMetric("some_name", "Description", Severity::INFO);
  auto TestSink = std::unique_ptr<Sink>(new MockSink());
  auto TestMockSink = dynamic_cast<MockSink *>(TestSink.get());
  Reporter TestReporter(std::move(TestSink), 10ms);

  ALLOW_CALL(*TestMockSink, reportMetric(_));

  std::string const FullName = "some_prefix.some_name";
  ASSERT_TRUE(TestReporter.addMetric(TestMetric, FullName));
  ASSERT_TRUE(TestReporter.tryRemoveMetric(FullName));
}

// cppcheck-suppress syntaxError
TEST(MetricsReporterTest, TryingToAddMetricWithSameFullNameTwiceFails) {
  Metric TestMetric1("some_name", "Description", Severity::INFO);
  Metric TestMetric2("some_name", "Different description", Severity::DEBUG);

  auto TestSink = std::unique_ptr<Sink>(new MockSink());
  auto TestMockSink = dynamic_cast<MockSink *>(TestSink.get());
  Reporter TestReporter(std::move(TestSink), 10ms);

  ALLOW_CALL(*TestMockSink, reportMetric(_));

  std::string const FullName = "some_prefix.some_name";
  ASSERT_TRUE(TestReporter.addMetric(TestMetric1, FullName));
  ASSERT_FALSE(TestReporter.addMetric(TestMetric2, FullName));
  TestReporter.tryRemoveMetric(FullName);
}

TEST(MetricsReporterTest, TryingToRemoveMetricWhichWasNotAddedFails) {
  auto TestSink = std::unique_ptr<Sink>(new MockSink());
  Reporter TestReporter(std::move(TestSink), 10ms);

  std::string const FullName = "some_prefix.some_name";
  ASSERT_FALSE(TestReporter.tryRemoveMetric(FullName));
}

TEST(MetricsReporterTest, AddedMetricIsReportedOn) {
  std::string const TestMetricName = "some_name";
  Metric TestMetric(TestMetricName, "Description", Severity::INFO);
  auto TestSink = std::unique_ptr<Sink>(new MockSink());
  auto TestMockSink = dynamic_cast<MockSink *>(TestSink.get());
  Reporter TestReporter(std::move(TestSink), 10ms);

  // Test reportMetric is called with a metric containing the name we set for
  // our test Metric
  REQUIRE_CALL(*TestMockSink, reportMetric(_)).WITH(_1.Name == TestMetricName);

  std::string const FullName = "some_prefix.some_name";
  TestReporter.addMetric(TestMetric, FullName);

  // Give the reporter plenty of time to report on the metric at least once.
  std::this_thread::sleep_for(50ms);

  TestReporter.tryRemoveMetric(FullName);
}

} // namespace Metrics
