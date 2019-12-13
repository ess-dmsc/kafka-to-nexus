#include <gtest/gtest.h>
#include "ProcessorStandIn.h"
#include "Metrics/Type.h"
#include <trompeloeil.hpp>

namespace Metrics {

class MetricsProcessorTest : public ::testing::Test {
public:
};

using std::string_literals::operator""s;

TEST_F(MetricsProcessorTest, RegisterNothing) {
  auto TestName = "SomeName"s;
  CounterType Ctr;
  auto Description = "Some description"s;
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {}));
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
  EXPECT_TRUE(UnderTest.LogMsgMetrics.empty());
}

TEST_F(MetricsProcessorTest, RegisterLogMsgCounter) {
  auto TestName = "SomeName"s;
  CounterType Ctr;
  auto Description = "Some description"s;
  ProcessorStandIn UnderTest;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {LogTo::LOG_MSG});
  EXPECT_EQ(UnderTest.LogMsgMetrics.size(), 1u);
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
}

TEST_F(MetricsProcessorTest, RegisterGrafanaCounter) {
  auto TestName = "SomeName"s;
  CounterType Ctr;
  auto Description = "Some description"s;
  ProcessorStandIn UnderTest;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {LogTo::GRAPHITE});
  EXPECT_EQ(UnderTest.GrafanaMetrics.size(), 1u);
  EXPECT_TRUE(UnderTest.LogMsgMetrics.empty());
}

TEST_F(MetricsProcessorTest, RegisterLogMsgAndGrafanaCounter) {
  auto TestName = "SomeName"s;
  CounterType Ctr;
  auto Description = "Some description"s;
  ProcessorStandIn UnderTest;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {LogTo::GRAPHITE, LogTo::LOG_MSG});
  EXPECT_EQ(UnderTest.GrafanaMetrics.size(), 1u);
  EXPECT_EQ(UnderTest.LogMsgMetrics.size(), 1u);
}

TEST_F(MetricsProcessorTest, SecondRegistrationSuccess) {
  auto TestName = "SomeName"s;
  auto TestName2 = "SomeName2"s;
  CounterType Ctr;
  auto Description = "Some description"s;
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {LogTo::LOG_MSG}));
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName2, &Ctr, Description, Severity::DEBUG, {LogTo::GRAPHITE}));
  EXPECT_EQ(UnderTest.GrafanaMetrics.size(), 1u);
  EXPECT_EQ(UnderTest.LogMsgMetrics.size(), 1u);
}

TEST_F(MetricsProcessorTest, SecondRegistrationFail) {
  auto TestName = "SomeName"s;
  CounterType Ctr;
  auto Description = "Some description"s;
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {LogTo::LOG_MSG}));
  EXPECT_FALSE(UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {LogTo::GRAPHITE}));
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
  EXPECT_EQ(UnderTest.LogMsgMetrics.size(), 1u);
}

} // namespace Metrics
