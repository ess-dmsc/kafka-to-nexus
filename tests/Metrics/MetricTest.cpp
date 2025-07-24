#include "Metrics/Metric.h"
#include "Metrics/Registrar.h"
#include "MockReporter.h"
#include "MockSink.h"
#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace Metrics {

class MetricTest : public ::testing::Test {};

// cppcheck-suppress syntaxError
TEST(MetricTest, Init) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  Metric UnderTest(NameStr, DescStr, TestSeverity);
  EXPECT_EQ(UnderTest.getName(), NameStr);
  EXPECT_EQ(UnderTest.getDescription(), DescStr);
  EXPECT_EQ(UnderTest.getSeverity(), TestSeverity);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
}

TEST(MetricTest, PreIncrement) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  Metric UnderTest(NameStr, DescStr, TestSeverity);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(++UnderTest, 1);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 1);
}

TEST(MetricTest, PostIncrement) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  Metric UnderTest(NameStr, DescStr, TestSeverity);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(UnderTest++, 1);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 1);
}

TEST(MetricTest, SumValue) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  Metric UnderTest(NameStr, DescStr, TestSeverity);
  auto TestValue = 42;
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(UnderTest += TestValue, TestValue);
  EXPECT_EQ(*UnderTest.getCounterPtr(), TestValue);
}

TEST(MetricTest, SetValue) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  Metric UnderTest(NameStr, DescStr, TestSeverity);
  auto TestValue = std::int64_t(42);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(UnderTest = TestValue, TestValue);
  EXPECT_EQ(*UnderTest.getCounterPtr(), TestValue);
}

TEST(MetricTest, Deregister) {
  using trompeloeil::_;

  std::string const NamePrefix = "Test";
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;

  auto TestSink = std::unique_ptr<Metrics::Sink>(new Metrics::MockSink());
  auto TestReporter = std::shared_ptr<Metrics::Reporter>(
      new MockReporter(std::move(TestSink), 10ms));
  std::vector<std::shared_ptr<Metrics::Reporter>> TestReporters{TestReporter};
  auto TestRegistrar = Metrics::Registrar(NamePrefix, TestReporters);

  auto TestReporterMock = std::dynamic_pointer_cast<MockReporter>(TestReporter);

  // Expect call to tryRemoveMetric when the metric gets deregisterd
  REQUIRE_CALL(*TestReporterMock, tryRemoveMetric(NamePrefix + "." + NameStr))
      .TIMES(1)
      .RETURN(true);
  ALLOW_CALL(*TestReporterMock, addMetric(_, _)).RETURN(true);

  {
    auto UnderTest = std::make_shared<Metric>(NameStr, DescStr, TestSeverity);
    TestRegistrar.registerMetric(UnderTest, {LogTo::LOG_MSG});
  } // When UnderTest Metric goes out of scope it should get deregistered
}
} // namespace Metrics
