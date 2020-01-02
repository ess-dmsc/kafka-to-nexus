#include <gtest/gtest.h>
#include "ProcessorStandIn.h"
#include "Metrics/Type.h"
#include <spdlog/spdlog.h>

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

TEST_F(MetricsProcessorTest, DeRegisterFail1) {
  auto TestName = "SomeName"s;
  ProcessorStandIn UnderTest;
  EXPECT_FALSE(UnderTest.deRegisterMetricBase(TestName));
}

TEST_F(MetricsProcessorTest, DeRegisterFail2) {
  auto TestName = "SomeName"s;
  CounterType Ctr;
  auto Description = "Some description"s;
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {LogTo::LOG_MSG, LogTo::GRAPHITE}));
  EXPECT_FALSE(UnderTest.deRegisterMetricBase("some_other_name"));
  EXPECT_EQ(UnderTest.GrafanaMetrics.size(), 1u);
  EXPECT_EQ(UnderTest.LogMsgMetrics.size(), 1u);
}

TEST_F(MetricsProcessorTest, DeRegisterFromBothSuccess) {
  auto TestName = "SomeName"s;
  CounterType Ctr;
  auto Description = "Some description"s;
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {LogTo::LOG_MSG, LogTo::GRAPHITE}));
  EXPECT_TRUE(UnderTest.deRegisterMetricBase("SomeName"));
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
  EXPECT_TRUE(UnderTest.LogMsgMetrics.empty());
}

TEST_F(MetricsProcessorTest, DeRegisterFromLogMsgSuccess) {
  auto TestName = "SomeName"s;
  CounterType Ctr;
  auto Description = "Some description"s;
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {LogTo::LOG_MSG}));
  EXPECT_TRUE(UnderTest.deRegisterMetricBase("SomeName"));
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
  EXPECT_TRUE(UnderTest.LogMsgMetrics.empty());
}

TEST_F(MetricsProcessorTest, DeRegisterFromGrafanaSuccess) {
  auto TestName = "SomeName"s;
  CounterType Ctr;
  auto Description = "Some description"s;
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG, {LogTo::GRAPHITE}));
  EXPECT_TRUE(UnderTest.deRegisterMetricBase("SomeName"));
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
  EXPECT_TRUE(UnderTest.LogMsgMetrics.empty());
}

class LoggerStandIn : public spdlog::logger {
public:
  LoggerStandIn(std::string SearchForString) : spdlog::logger("unused_name", nullptr), ExpectedSubString(SearchForString) {}
   void sink_it_(spdlog::details::log_msg &Msg) override {
    auto LastMessage = std::string(Msg.payload.data(), Msg.payload.size());
    if (LastMessage.find(ExpectedSubString) != std::string::npos) {
      MessagesWithSubString++;
    }
  }
  int MessagesWithSubString{0};
  std::string ExpectedSubString;
};

using trompeloeil::_;

TEST_F(MetricsProcessorTest, NoLogMsgUpdate1) {
  auto TestName = "SomeLongWindedName"s;
  CounterType Ctr{0};
  auto Description = "A long description of a metric."s;
  ProcessorStandIn UnderTest(1ms);
  auto UsedLogger = std::make_shared<LoggerStandIn>(TestName);
  UnderTest.Logger = UsedLogger;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR, {LogTo::LOG_MSG});
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 1);
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 1);
}

TEST_F(MetricsProcessorTest, NoLogMsgUpdate3) {
  auto TestName = "SomeLongWindedName"s;
  CounterType Ctr{0};
  auto Description = "A long description of a metric."s;
  ProcessorStandIn UnderTest(1ms);
  auto UsedLogger = std::make_shared<LoggerStandIn>(TestName);
  UnderTest.Logger = UsedLogger;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR, {LogTo::GRAPHITE});
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 1);
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 1);
}

TEST_F(MetricsProcessorTest, NoLogMsgUpdate2) {
  auto TestName = "SomeLongWindedName"s;
  CounterType Ctr{0};
  auto Description = "A long description of a metric."s;
  ProcessorStandIn UnderTest(1ms);
  auto UsedLogger = std::make_shared<LoggerStandIn>(TestName);
  UnderTest.Logger = UsedLogger;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR, {});
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 0);
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 0);
}

TEST_F(MetricsProcessorTest, OneLogMsgUpate) {
  auto TestName = "SomeLongWindedName"s;
  CounterType Ctr{0};
  auto Description = "A long description of a metric."s;
  ProcessorStandIn UnderTest(1ms);
  auto UsedLogger = std::make_shared<LoggerStandIn>(TestName);
  UnderTest.Logger = UsedLogger;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR, {LogTo::LOG_MSG});
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 1);
  Ctr = 5;
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 2);
}

} // namespace Metrics
