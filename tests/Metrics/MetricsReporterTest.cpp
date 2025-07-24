#include "Metrics/Reporter.h"
#include "MockSink.h"
#include <chrono>
#include <gtest/gtest.h>

using namespace std::chrono_literals;
using trompeloeil::_;

class MetricsReporterTest : public ::testing::Test {};

namespace Metrics {

class MockUnhealthySink : public Sink {
public:
  explicit MockUnhealthySink(LogTo LogToSink = LogTo::LOG_MSG)
      : SinkType(LogToSink) {};
  MAKE_MOCK1(reportMetric, void(InternalMetric &), override);
  LogTo getType() const override { return SinkType; };
  bool isHealthy() const override { return false; };

private:
  LogTo SinkType;
};

// cppcheck-suppress syntaxError
TEST(MetricsReporterTest,
     MetricSuccessfullyAddedCanBeRemovedUsingSameFullName) {
  auto TestMetric =
      std::make_shared<Metric>("some_name", "Description", Severity::INFO);
  auto TestSink = std::unique_ptr<Sink>(new MockSink());
  auto TestMockSink = dynamic_cast<MockSink *>(TestSink.get());
  Reporter TestReporter(std::move(TestSink), 10ms);

  ALLOW_CALL(*TestMockSink, reportMetric(_));

  std::string const FullName = "some_prefix.some_name";
  ASSERT_TRUE(TestReporter.addMetric(TestMetric, FullName));
  ASSERT_TRUE(TestReporter.tryRemoveMetric(FullName));
}

TEST(MetricsReporterTest, TryingToAddMetricWithSameFullNameTwiceFails) {
  auto TestMetric1 =
      std::make_shared<Metric>("some_name", "Description", Severity::INFO);
  auto TestMetric2 = std::make_shared<Metric>(
      "some_name", "Different description", Severity::DEBUG);

  auto TestSink = std::unique_ptr<Sink>(new MockSink());
  auto TestMockSink = dynamic_cast<MockSink *>(TestSink.get());
  Reporter TestReporter(std::move(TestSink), 10ms);

  ALLOW_CALL(*TestMockSink, reportMetric(_));

  std::string const FullName = "some_prefix.some_name";
  ASSERT_TRUE(TestReporter.addMetric(TestMetric1, FullName));
  ASSERT_FALSE(TestReporter.addMetric(TestMetric1, FullName));
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
  auto TestMetric =
      std::make_shared<Metric>(TestMetricName, "Description", Severity::INFO);
  auto TestSink = std::unique_ptr<Sink>(new MockSink());
  auto TestMockSink = dynamic_cast<MockSink *>(TestSink.get());
  Reporter TestReporter(std::move(TestSink), 10ms);

  // Test reportMetric is called with a metric containing the name we set for
  // our test Metric
  REQUIRE_CALL(*TestMockSink, reportMetric(_))
      .WITH(_1.Name == TestMetricName)
      .TIMES(AT_LEAST(2));

  std::string const FullName = "some_prefix.some_name";
  TestReporter.addMetric(TestMetric, FullName);

  // Give the reporter plenty of time to report on the metric at least twice.
  std::this_thread::sleep_for(100ms);

  TestReporter.tryRemoveMetric(FullName);
}

TEST(MetricsReporterTest, DoesNotReportMetricsIfSinkIsNotHealthy) {
  auto TestMetric =
      std::make_shared<Metric>("some_name", "Description", Severity::INFO);
  auto TestSink = std::unique_ptr<Sink>(new MockUnhealthySink());
  auto TestMockSink = dynamic_cast<MockUnhealthySink *>(TestSink.get());
  Reporter TestReporter(std::move(TestSink), 10ms);

  FORBID_CALL(*TestMockSink, reportMetric(_));

  std::string const FullName = "some_prefix.some_name";
  TestReporter.addMetric(TestMetric, FullName);

  // Give the reporter plenty of time to report on the metric. But it shouldn't
  // because the Sink claims to be unhealthy.
  std::this_thread::sleep_for(100ms);

  TestReporter.tryRemoveMetric(FullName);
}

} // namespace Metrics
