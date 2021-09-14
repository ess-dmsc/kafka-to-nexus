#include "Metrics/InternalMetric.h"
#include "Metrics/LogSink.h"
#include "Metrics/Metric.h"
#include <graylog_logger/ConsoleInterface.hpp>
#include <graylog_logger/LogUtil.hpp>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

using trompeloeil::_;
using trompeloeil::re;

bool messageContainsSubstring(Log::LogMessage const &InputStringMessage,
                              std::string const &InputSubstring) {
  return InputStringMessage.MessageString.find(InputSubstring) !=
         std::string::npos;
}

class MockLogger : public Log::BaseLogHandler {
public:
  MockLogger() = default;
  MAKE_MOCK1(addMessage, void(Log::LogMessage const &), override);
  MAKE_MOCK1(flush, bool(std::chrono::system_clock::duration), override);
  MAKE_MOCK0(emptyQueue, bool(), override);
  MAKE_MOCK0(queueSize, size_t(), override);
};

class LogSinkTest : public ::testing::Test {
public:
  void SetUp() override {
    ASSERT_TRUE(Log::Flush());
    LoggerUnderTest = std::make_shared<MockLogger>();
    Log::AddLogHandler(LoggerUnderTest);
  }

  void TearDown() override {
    Log::RemoveAllHandlers();
    Log::AddLogHandler(new Log::ConsoleInterface());
    LoggerUnderTest.reset();
  }
  std::shared_ptr<MockLogger> LoggerUnderTest;
};

namespace Metrics {

TEST_F(LogSinkTest, LogSinkReportsSinkTypeAsLog) {
  LogSink TestLogSink{};
  ASSERT_EQ(TestLogSink.getType(), LogTo::LOG_MSG);
}

TEST_F(LogSinkTest, NothingIsLoggedIfMetricDidNotIncrement) {
  LogSink TestLogSink{};
  std::string const TestMetricName = "some_name";
  std::string const TestMetricDescription = "metric description";
  Severity const TestMetricSeverity = Severity::INFO;
  Metric TestMetric{TestMetricName, TestMetricDescription, TestMetricSeverity};
  std::string const TestMetricFullName = "prefix." + TestMetricName;
  InternalMetric TestInternalMetric{TestMetric, TestMetricFullName};
  TestInternalMetric.LastValue = 0;

  // InternalMetric's LastValue is same as current Metric counter value,
  // therefore nothing should be logged
  FORBID_CALL(*LoggerUnderTest, addMessage(_));
  ALLOW_CALL(*LoggerUnderTest, flush(_)).RETURN(true);

  TestLogSink.reportMetric(TestInternalMetric);
  ASSERT_TRUE(Log::Flush());
}

TEST_F(LogSinkTest, LogsIfMetricHasIncremented) {
  LogSink TestLogSink{};
  std::string const TestMetricName = "some_name";
  std::string const TestMetricDescription = "metric description";
  Severity const TestMetricSeverity = Severity::ERROR;
  Metric TestMetric{TestMetricName, TestMetricDescription, TestMetricSeverity};
  std::string const TestMetricFullName = "prefix." + TestMetricName;
  InternalMetric TestInternalMetric{TestMetric, TestMetricFullName};
  TestInternalMetric.LastValue = 0;

  TestMetric++;

  // InternalMetric's LastValue is different to current Metric counter value,
  // therefore expect a message to be logged containing our metric details
  REQUIRE_CALL(*LoggerUnderTest, addMessage(_))
      .WITH(messageContainsSubstring(_1, TestMetricName) &&
            messageContainsSubstring(_1, TestMetricDescription));
  ALLOW_CALL(*LoggerUnderTest, flush(_)).RETURN(true);

  TestLogSink.reportMetric(TestInternalMetric);
  ASSERT_TRUE(Log::Flush());
}

} // namespace Metrics
