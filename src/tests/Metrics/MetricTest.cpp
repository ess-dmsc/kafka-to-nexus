#include "Metrics/Metric.h"
#include "Metrics/Registrar.h"
#include <gtest/gtest.h>

namespace Metrics {

class MetricTest : public ::testing::Test {
public:
};

TEST_F(MetricTest, Init) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  Metric UnderTest(NameStr, DescStr, TestSeverity);
  EXPECT_EQ(UnderTest.getName(), NameStr);
  EXPECT_EQ(UnderTest.getDescription(), DescStr);
  EXPECT_EQ(UnderTest.getSeverity(), TestSeverity);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
}

TEST_F(MetricTest, PreIncrement) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  Metric UnderTest(NameStr, DescStr, TestSeverity);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(++UnderTest, 1);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 1);
}

TEST_F(MetricTest, PostIncrement) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  Metric UnderTest(NameStr, DescStr, TestSeverity);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(UnderTest++, 1);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 1);
}

TEST_F(MetricTest, SumValue) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  Metric UnderTest(NameStr, DescStr, TestSeverity);
  auto TestValue = 42;
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(UnderTest += TestValue, TestValue);
  EXPECT_EQ(*UnderTest.getCounterPtr(), TestValue);
}

TEST_F(MetricTest, SetValue) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  Metric UnderTest(NameStr, DescStr, TestSeverity);
  auto TestValue = std::int64_t(42);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(UnderTest = TestValue, TestValue);
  EXPECT_EQ(*UnderTest.getCounterPtr(), TestValue);
}

TEST_F(MetricTest, Deregister) {
  auto NameStr = std::string("test_name");
  auto DescStr = std::string("some_description");
  auto TestSeverity = Severity::ERROR;
  auto TestRegistrar = std::make_shared<Metrics::Registrar>();

  {
    Metric UnderTest(TestRegistrar, NameStr, DescStr, TestSeverity);
    TestRegistrar->registerMetric(UnderTest, {LogTo::LOG_MSG});

    // Test UnderTest is in MetricList
    auto ListOfMetrics = TestMetricsList->getListOfMetrics();
    EXPECT_TRUE(ListOfMetrics.find(NameStr) != ListOfMetrics.end())
        << "Expect to find the metric in the list";
  } // UnderTest Metric goes out of scope so should get deregistered

  auto ListOfMetrics = TestMetricsList->getListOfMetrics();
  EXPECT_TRUE(ListOfMetrics.find(NameStr) == ListOfMetrics.end())
      << "Expect not to find the metric in the list";
}

} // namespace Metrics
