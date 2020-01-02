#include "Metrics/Type.h"
#include "ProcessorStandIn.h"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

namespace Metrics {

class MetricsProcessorTest : public ::testing::Test {
public:
};


TEST_F(MetricsProcessorTest, RegisterNothing) {
  auto TestName = std::string("SomeName");
  CounterType Ctr;
  auto Description = std::string("Some description");
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description,
                                           Severity::DEBUG, {}));
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
  EXPECT_TRUE(UnderTest.LogMsgMetrics.empty());
}

TEST_F(MetricsProcessorTest, RegisterLogMsgCounter) {
  auto TestName = std::string("SomeName");
  CounterType Ctr;
  auto Description = std::string("Some description");
  ProcessorStandIn UnderTest;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG,
                               {LogTo::LOG_MSG});
  EXPECT_EQ(UnderTest.LogMsgMetrics.size(), 1u);
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
}

TEST_F(MetricsProcessorTest, RegisterGrafanaCounter) {
  auto TestName = std::string("SomeName");
  CounterType Ctr;
  auto Description = std::string("Some description");
  ProcessorStandIn UnderTest;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG,
                               {LogTo::CARBON});
  EXPECT_EQ(UnderTest.GrafanaMetrics.size(), 1u);
  EXPECT_TRUE(UnderTest.LogMsgMetrics.empty());
}

TEST_F(MetricsProcessorTest, RegisterLogMsgAndGrafanaCounter) {
  auto TestName = std::string("SomeName");
  CounterType Ctr;
  auto Description = std::string("Some description");
  ProcessorStandIn UnderTest;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::DEBUG,
                               {LogTo::CARBON, LogTo::LOG_MSG});
  EXPECT_EQ(UnderTest.GrafanaMetrics.size(), 1u);
  EXPECT_EQ(UnderTest.LogMsgMetrics.size(), 1u);
}

TEST_F(MetricsProcessorTest, SecondRegistrationSuccess) {
  auto TestName = std::string("SomeName");
  auto TestName2 = std::string("SomeName2");
  CounterType Ctr;
  auto Description = std::string("Some description");
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description,
                                           Severity::DEBUG, {LogTo::LOG_MSG}));
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName2, &Ctr, Description,
                                           Severity::DEBUG, {LogTo::CARBON}));
  EXPECT_EQ(UnderTest.GrafanaMetrics.size(), 1u);
  EXPECT_EQ(UnderTest.LogMsgMetrics.size(), 1u);
}

TEST_F(MetricsProcessorTest, SecondRegistrationFail) {
  auto TestName = std::string("SomeName");
  CounterType Ctr;
  auto Description = std::string("Some description");
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description,
                                           Severity::DEBUG, {LogTo::LOG_MSG}));
  EXPECT_FALSE(UnderTest.registerMetricBase(TestName, &Ctr, Description,
                                            Severity::DEBUG, {LogTo::CARBON}));
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
  EXPECT_EQ(UnderTest.LogMsgMetrics.size(), 1u);
}

TEST_F(MetricsProcessorTest, DeRegisterFail1) {
  auto TestName = std::string("SomeName");
  ProcessorStandIn UnderTest;
  EXPECT_FALSE(UnderTest.deRegisterMetricBase(TestName));
}

TEST_F(MetricsProcessorTest, DeRegisterFail2) {
  auto TestName = std::string("SomeName");
  CounterType Ctr;
  auto Description = std::string("Some description");
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description,
                                           Severity::DEBUG,
                                           {LogTo::LOG_MSG, LogTo::CARBON}));
  EXPECT_FALSE(UnderTest.deRegisterMetricBase("some_other_name"));
  EXPECT_EQ(UnderTest.GrafanaMetrics.size(), 1u);
  EXPECT_EQ(UnderTest.LogMsgMetrics.size(), 1u);
}

TEST_F(MetricsProcessorTest, DeRegisterFromBothSuccess) {
  auto TestName = std::string("SomeName");
  CounterType Ctr;
  auto Description = std::string("Some description");
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description,
                                           Severity::DEBUG,
                                           {LogTo::LOG_MSG, LogTo::CARBON}));
  EXPECT_TRUE(UnderTest.deRegisterMetricBase("SomeName"));
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
  EXPECT_TRUE(UnderTest.LogMsgMetrics.empty());
}

TEST_F(MetricsProcessorTest, DeRegisterFromLogMsgSuccess) {
  auto TestName = std::string("SomeName");
  CounterType Ctr;
  auto Description = std::string("Some description");
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description,
                                           Severity::DEBUG, {LogTo::LOG_MSG}));
  EXPECT_TRUE(UnderTest.deRegisterMetricBase("SomeName"));
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
  EXPECT_TRUE(UnderTest.LogMsgMetrics.empty());
}

TEST_F(MetricsProcessorTest, DeRegisterFromGrafanaSuccess) {
  auto TestName = std::string("SomeName");
  CounterType Ctr;
  auto Description = std::string("Some description");
  ProcessorStandIn UnderTest;
  EXPECT_TRUE(UnderTest.registerMetricBase(TestName, &Ctr, Description,
                                           Severity::DEBUG, {LogTo::CARBON}));
  EXPECT_TRUE(UnderTest.deRegisterMetricBase("SomeName"));
  EXPECT_TRUE(UnderTest.GrafanaMetrics.empty());
  EXPECT_TRUE(UnderTest.LogMsgMetrics.empty());
}

class LoggerStandIn : public spdlog::logger {
public:
  LoggerStandIn(std::string SearchForString)
      : spdlog::logger("unused_name", nullptr),
        ExpectedSubString(SearchForString) {}
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
  auto TestName = std::string("SomeLongWindedName");
  CounterType Ctr{0};
  auto Description = std::string("A long description of a metric.");
  ProcessorStandIn UnderTest(1ms);
  auto UsedLogger = std::make_shared<LoggerStandIn>(TestName);
  UnderTest.Logger = UsedLogger;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR,
                               {LogTo::LOG_MSG});
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 1);
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 1);
}

TEST_F(MetricsProcessorTest, NoLogMsgUpdate2) {
  auto TestName = std::string("SomeLongWindedName");
  CounterType Ctr{0};
  auto Description = std::string("A long description of a metric.");
  ProcessorStandIn UnderTest(1ms);
  auto UsedLogger = std::make_shared<LoggerStandIn>(TestName);
  UnderTest.Logger = UsedLogger;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR,
                               {});
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 0);
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 0);
}

TEST_F(MetricsProcessorTest, NoLogMsgUpdate3) {
  auto TestName = std::string("SomeLongWindedName");
  CounterType Ctr{0};
  auto Description = std::string("A long description of a metric.");
  ProcessorStandIn UnderTest(1ms);
  auto UsedLogger = std::make_shared<LoggerStandIn>(TestName);
  UnderTest.Logger = UsedLogger;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR,
                               {LogTo::CARBON});
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 1);
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 1);
}

TEST_F(MetricsProcessorTest, OneLogMsgUpate) {
  auto TestName = std::string("SomeLongWindedName");
  CounterType Ctr{0};
  auto Description = std::string("A long description of a metric.");
  ProcessorStandIn UnderTest(1ms);
  auto UsedLogger = std::make_shared<LoggerStandIn>(TestName);
  UnderTest.Logger = UsedLogger;
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR,
                               {LogTo::LOG_MSG});
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 1);
  Ctr = 5;
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(UsedLogger->MessagesWithSubString, 2);
}

using trompeloeil::_;

TEST_F(MetricsProcessorTest, NoCarbonValueChange) {
  auto TestName = std::string("SomeLongWindedName");
  CounterType Ctr{0};
  auto Description = std::string("A long description of a metric.");
  ProcessorStandIn UnderTest(10000ms, 1ms);
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR,
                               {LogTo::CARBON});
  REQUIRE_CALL(UnderTest, sendMsgToCarbon(TestName, InternalCounterType(0), _))
      .TIMES(AT_LEAST(1));
  std::this_thread::sleep_for(50ms);
}

TEST_F(MetricsProcessorTest, NoCarbonMsg1) {
  auto TestName = std::string("SomeLongWindedName");
  CounterType Ctr{0};
  auto Description = std::string("A long description of a metric.");
  ProcessorStandIn UnderTest(10000ms, 1ms);
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR,
                               {LogTo::LOG_MSG});
  FORBID_CALL(UnderTest, sendMsgToCarbon(_, _, _));
  std::this_thread::sleep_for(50ms);
}

TEST_F(MetricsProcessorTest, NoCarbonMsg2) {
  auto TestName = std::string("SomeLongWindedName");
  CounterType Ctr{0};
  auto Description = std::string("A long description of a metric.");
  ProcessorStandIn UnderTest(10000ms, 1ms);
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR,
                               {});
  FORBID_CALL(UnderTest, sendMsgToCarbon(_, _, _));
  std::this_thread::sleep_for(50ms);
}

TEST_F(MetricsProcessorTest, CarbonUpdate) {
  auto TestName = std::string("SomeLongWindedName");
  CounterType Ctr{0};
  auto Description = std::string("A long description of a metric.");
  ProcessorStandIn UnderTest(10000ms, 1ms);
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR,
                               {LogTo::CARBON});
  ALLOW_CALL(UnderTest, sendMsgToCarbon(TestName, InternalCounterType(0), _));
  REQUIRE_CALL(UnderTest, sendMsgToCarbon(TestName, InternalCounterType(5), _))
      .TIMES(AT_LEAST(1));
  Ctr = 5;
  std::this_thread::sleep_for(50ms);
}

TEST_F(MetricsProcessorTest, QueuedCarbonMsg) {
  auto TestName = std::string("SomeLongWindedName");
  CounterType Ctr{0};
  auto Description = std::string("A long description of a metric.");
  ProcessorStandIn UnderTest(10000ms, 1ms);
  UnderTest.registerMetricBase(TestName, &Ctr, Description, Severity::ERROR,
                               {LogTo::CARBON});
  UnderTest.sendMsgToCarbonBase(TestName, 11, std::chrono::system_clock::now());
  FORBID_CALL(UnderTest, sendMsgToCarbon(_, _, _));
  Ctr = 5;
  std::this_thread::sleep_for(50ms);
}

} // namespace Metrics
