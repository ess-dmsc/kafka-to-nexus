#include "Metrics/InternalMetric.h"
#include "Metrics/LogSink.h"
#include "Metrics/Metric.h"
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

using trompeloeil::_;
using trompeloeil::re;

class LogSinkTest : public ::testing::Test {};

bool messageContainsSubstring(spdlog::details::log_msg &InputStringMessage,
                              std::string const &InputSubstring) {
  return std::string(InputStringMessage.payload.data(),
                     InputStringMessage.payload.size())
             .find(InputSubstring) != std::string::npos;
}

class MockLogger : public spdlog::logger {
public:
  MockLogger() : spdlog::logger("unused_name", nullptr) {}
  MAKE_MOCK1(sink_it_, void(spdlog::details::log_msg &), override);
};

namespace Metrics {

TEST(LogSinkTest, LogSinkReportsSinkTypeAsLog) {
  LogSink TestLogSink{};
  ASSERT_EQ(TestLogSink.getType(), LogTo::LOG_MSG);
}

// cppcheck-suppress syntaxError
TEST(LogSinkTest, NothingIsLoggedIfMetricDidNotIncrement) {
  LogSink TestLogSink{};
  TestLogSink.Logger = std::shared_ptr<spdlog::logger>(new MockLogger());
  auto LoggerMock = dynamic_cast<MockLogger *>(TestLogSink.Logger.get());

  std::string const TestMetricName = "some_name";
  std::string const TestMetricDescription = "metric description";
  Severity const TestMetricSeverity = Severity::INFO;
  Metric TestMetric{TestMetricName, TestMetricDescription, TestMetricSeverity};
  std::string const TestMetricFullName = "prefix." + TestMetricName;
  InternalMetric TestInternalMetric{TestMetric, TestMetricFullName};
  TestInternalMetric.LastValue = 0;

  // InternalMetric's LastValue is same as current Metric counter value,
  // therefore nothing should be logged
  FORBID_CALL(*LoggerMock, sink_it_(_));

  TestLogSink.reportMetric(TestInternalMetric);
}

TEST(LogSinkTest, LogsIfMetricHasIncremented) {
  LogSink TestLogSink{};
  TestLogSink.Logger = std::shared_ptr<spdlog::logger>(new MockLogger());
  auto LoggerMock = dynamic_cast<MockLogger *>(TestLogSink.Logger.get());

  std::string const TestMetricName = "some_name";
  std::string const TestMetricDescription = "metric description";
  Severity const TestMetricSeverity = Severity::INFO;
  Metric TestMetric{TestMetricName, TestMetricDescription, TestMetricSeverity};
  std::string const TestMetricFullName = "prefix." + TestMetricName;
  InternalMetric TestInternalMetric{TestMetric, TestMetricFullName};
  TestInternalMetric.LastValue = 0;

  TestMetric++;

  // InternalMetric's LastValue is different to current Metric counter value,
  // therefore expect a message to be logged containing our metric details
  REQUIRE_CALL(*LoggerMock, sink_it_(_))
      .WITH(messageContainsSubstring(_1, TestMetricName) &&
            messageContainsSubstring(_1, TestMetricDescription));

  TestLogSink.reportMetric(TestInternalMetric);
}

} // namespace Metrics
