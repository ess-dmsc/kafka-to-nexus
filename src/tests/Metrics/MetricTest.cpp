#include "MetricStandIn.h"
#include "Metrics/Processor.h"
#include "Metrics/Type.h"
#include "ProcessorStandIn.h"
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

namespace Metrics {

class MetricTest : public ::testing::Test {
public:
};

using std::string_literals::operator""s;

TEST_F(MetricTest, Init) {
  auto NameStr = "test_name"s;
  auto DescStr = "some_description"s;
  auto TestSeverity = Severity::ERROR;
  MetricStandIn UnderTest(NameStr, DescStr, TestSeverity);
  EXPECT_EQ(UnderTest.getName(), NameStr);
  EXPECT_EQ(UnderTest.getDescription(), DescStr);
  EXPECT_EQ(UnderTest.getSeverity(), TestSeverity);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
}

TEST_F(MetricTest, PreIncrement) {
  auto NameStr = "test_name"s;
  auto DescStr = "some_description"s;
  auto TestSeverity = Severity::ERROR;
  MetricStandIn UnderTest(NameStr, DescStr, TestSeverity);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(++UnderTest, 1);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 1);
}

TEST_F(MetricTest, PostIncrement) {
  auto NameStr = "test_name"s;
  auto DescStr = "some_description"s;
  auto TestSeverity = Severity::ERROR;
  MetricStandIn UnderTest(NameStr, DescStr, TestSeverity);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(UnderTest++, 1);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 1);
}

TEST_F(MetricTest, SumValue) {
  auto NameStr = "test_name"s;
  auto DescStr = "some_description"s;
  auto TestSeverity = Severity::ERROR;
  MetricStandIn UnderTest(NameStr, DescStr, TestSeverity);
  auto TestValue = 42;
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(UnderTest += TestValue, TestValue);
  EXPECT_EQ(*UnderTest.getCounterPtr(), TestValue);
}

TEST_F(MetricTest, SetValue) {
  auto NameStr = "test_name"s;
  auto DescStr = "some_description"s;
  auto TestSeverity = Severity::ERROR;
  MetricStandIn UnderTest(NameStr, DescStr, TestSeverity);
  auto TestValue = std::int64_t(42);
  EXPECT_EQ(*UnderTest.getCounterPtr(), 0);
  EXPECT_EQ(UnderTest = TestValue, TestValue);
  EXPECT_EQ(*UnderTest.getCounterPtr(), TestValue);
}

TEST_F(MetricTest, DeRegister) {
  auto NameStr = "test_name"s;
  auto DescStr = "some_description"s;
  auto TestSeverity = Severity::ERROR;
  auto SomeDeRegName = "some other string"s;
  ProcessorStandIn TestProcessor;
  REQUIRE_CALL(TestProcessor, deRegisterMetric(SomeDeRegName))
      .TIMES(1)
      .RETURN(true);
  {
    MetricStandIn UnderTest(NameStr, DescStr, TestSeverity);
    UnderTest.setDeRegParams(
        SomeDeRegName, dynamic_cast<ProcessorInterface *>(&TestProcessor));
  }
}

} // namespace Metrics