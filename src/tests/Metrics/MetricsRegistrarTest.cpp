#include "MockReporter.h"
#include "MockSink.h"
#include <Metrics/Registrar.h>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

using namespace std::chrono_literals;

namespace Metrics {

class MetricsRegistrarTest : public ::testing::Test {
public:
  void SetUp() override {
    auto TestSink = std::unique_ptr<Metrics::Sink>(new Metrics::MockSink());
    auto TestReporter = std::shared_ptr<Metrics::Reporter>(
        new MockReporter(std::move(TestSink), 10ms));
    TestReporters = {TestReporter};
  }
  std::vector<std::shared_ptr<Metrics::Reporter>> TestReporters;
};

using trompeloeil::_;

// cppcheck-suppress syntaxError
TEST_F(MetricsRegistrarTest, RegisteringANewMetricAddsItToTheReporter) {
  std::string const Name = "some_name";
  std::string const Desc = "Description";
  auto const Sev = Severity::INFO;

  std::string const EmptyPrefix;
  auto TestRegistrar = Metrics::Registrar(EmptyPrefix, TestReporters);
  auto TestReporterMock =
      std::dynamic_pointer_cast<MockReporter>(TestReporters[0]);

  REQUIRE_CALL(*TestReporterMock, addMetric(_, Name)).TIMES(1).RETURN(true);
  // Allow deregister call when Metric goes out of scope
  ALLOW_CALL(*TestReporterMock, tryRemoveMetric(Name)).RETURN(true);

  {
    Metric TestMetric(Name, Desc, Sev);
    TestRegistrar.registerMetric(TestMetric, {LogTo::LOG_MSG});
  }
}

TEST_F(MetricsRegistrarTest, RegisterAndDeregisterWithMetricNamePrefix) {
  std::string const BasePrefix = "some_name.";
  std::string const ExtraPrefix = "some_prefix";
  std::string const Name = "some_metric";
  std::string const Desc = "Description";
  auto const Sev = Severity::INFO;
  auto const FullName = BasePrefix + "." + ExtraPrefix + "." + Name;

  auto TestRegistrar = Metrics::Registrar(BasePrefix, TestReporters);
  auto TestRegistrarExtraPrefix = TestRegistrar.getNewRegistrar(ExtraPrefix);
  auto TestReporterMock =
      std::dynamic_pointer_cast<MockReporter>(TestReporters[0]);

  REQUIRE_CALL(*TestReporterMock, addMetric(_, FullName)).TIMES(1).RETURN(true);
  REQUIRE_CALL(*TestReporterMock, tryRemoveMetric(FullName))
      .TIMES(1)
      .RETURN(true);
  {
    Metric Ctr(Name, Desc, Sev);
    TestRegistrarExtraPrefix->registerMetric(Ctr, {LogTo::LOG_MSG});
  }
}

TEST_F(MetricsRegistrarTest, RegisterWithEmptyNameFails) {
  std::string const EmptyName;
  std::string const Desc = "Description";
  auto const Sev = Severity::INFO;

  std::string const EmptyPrefix;
  auto TestRegistrar = Metrics::Registrar(EmptyPrefix, TestReporters);

  auto TestReporterMock =
      std::dynamic_pointer_cast<MockReporter>(TestReporters[0]);

  FORBID_CALL(*TestReporterMock, addMetric(_, _));
  FORBID_CALL(*TestReporterMock, tryRemoveMetric(_));
  {
    Metric Ctr(EmptyName, Desc, Sev);
    EXPECT_THROW(TestRegistrar.registerMetric(Ctr, {LogTo::LOG_MSG}),
                 std::runtime_error)
        << "Expect registering metric with empty name to fail";
  }
}

TEST_F(MetricsRegistrarTest, RegisterWithExistingNameFails) {
  std::string const Name = "metric_name";
  std::string const Desc = "Description";
  auto const Sev = Severity::INFO;

  auto TestSink = std::unique_ptr<Metrics::Sink>(new Metrics::MockSink());
  auto TestReporter =
      std::make_shared<Metrics::Reporter>(std::move(TestSink), 10ms);
  std::vector<std::shared_ptr<Metrics::Reporter>> TestReporterList{
      TestReporter};

  std::string const EmptyPrefix;
  auto TestRegistrar = Metrics::Registrar(EmptyPrefix, TestReporterList);

  {
    Metric TestMetric(Name, Desc, Sev);
    TestRegistrar.registerMetric(TestMetric, {LogTo::LOG_MSG});
    Metric TestMetricWithSameName(Name, Desc, Sev);
    EXPECT_THROW(
        TestRegistrar.registerMetric(TestMetricWithSameName, {LogTo::LOG_MSG}),
        std::runtime_error)
        << "Expect registering metric with existing name to fail";
  }
}

TEST_F(MetricsRegistrarTest, RegisteringLogMetricAddsToReporterWithLogSink) {
  std::string const Name = "some_name";
  std::string const Desc = "Description";
  auto const Sev = Severity::INFO;

  // Make a reporter with a logger sink
  auto TestLogSink = std::unique_ptr<Metrics::Sink>(
      new Metrics::MockSink(Metrics::LogTo::LOG_MSG));
  auto TestLogReporter = std::shared_ptr<Metrics::Reporter>(
      new Metrics::MockReporter(std::move(TestLogSink), 10ms));

  // Make a reporter with a Carbon sink
  auto TestCarbonSink = std::unique_ptr<Metrics::Sink>(
      new Metrics::MockSink(Metrics::LogTo::CARBON));
  auto TestCarbonReporter = std::shared_ptr<Metrics::Reporter>(
      new Metrics::MockReporter(std::move(TestCarbonSink), 10ms));

  std::vector<std::shared_ptr<Metrics::Reporter>> TestReporterList = {
      TestLogReporter, TestCarbonReporter};

  std::string const EmptyPrefix;
  auto TestRegistrar = Metrics::Registrar(EmptyPrefix, TestReporterList);
  auto TestLogReporterMock =
      std::dynamic_pointer_cast<MockReporter>(TestLogReporter);
  auto TestCarbonReporterMock =
      std::dynamic_pointer_cast<MockReporter>(TestCarbonReporter);

  REQUIRE_CALL(*TestLogReporterMock, addMetric(_, Name)).TIMES(1).RETURN(true);
  // Allow deregister call when Metric goes out of scope
  ALLOW_CALL(*TestLogReporterMock, tryRemoveMetric(Name)).RETURN(true);
  FORBID_CALL(*TestCarbonReporterMock, addMetric(_, Name));
  {
    Metric TestMetric(Name, Desc, Sev);
    TestRegistrar.registerMetric(TestMetric, {LogTo::LOG_MSG});
  }
}

TEST_F(MetricsRegistrarTest,
       RegisteringCarbonMetricAddsToReporterWithCarbonSink) {
  std::string const Name = "some_name";
  std::string const Desc = "Description";
  auto const Sev = Severity::INFO;

  // Make a reporter with a logger sink
  auto TestLogSink = std::unique_ptr<Metrics::Sink>(
      new Metrics::MockSink(Metrics::LogTo::LOG_MSG));
  auto TestLogReporter = std::shared_ptr<Metrics::Reporter>(
      new Metrics::MockReporter(std::move(TestLogSink), 10ms));

  // Make a reporter with a Carbon sink
  auto TestCarbonSink = std::unique_ptr<Metrics::Sink>(
      new Metrics::MockSink(Metrics::LogTo::CARBON));
  auto TestCarbonReporter = std::shared_ptr<Metrics::Reporter>(
      new Metrics::MockReporter(std::move(TestCarbonSink), 10ms));

  std::vector<std::shared_ptr<Metrics::Reporter>> TestReporterList = {
      TestLogReporter, TestCarbonReporter};

  std::string const EmptyPrefix;
  auto TestRegistrar = Metrics::Registrar(EmptyPrefix, TestReporterList);
  auto TestLogReporterMock =
      std::dynamic_pointer_cast<MockReporter>(TestLogReporter);
  auto TestCarbonReporterMock =
      std::dynamic_pointer_cast<MockReporter>(TestCarbonReporter);

  REQUIRE_CALL(*TestCarbonReporterMock, addMetric(_, Name))
      .TIMES(1)
      .RETURN(true);
  // Allow deregister call when Metric goes out of scope
  ALLOW_CALL(*TestCarbonReporterMock, tryRemoveMetric(Name)).RETURN(true);
  FORBID_CALL(*TestLogReporterMock, addMetric(_, Name));
  {
    Metric TestMetric(Name, Desc, Sev);
    TestRegistrar.registerMetric(TestMetric, {LogTo::CARBON});
  }
}

TEST_F(MetricsRegistrarTest, RegisterMetricToMultipleReporters) {
  std::string const Name = "some_name";
  std::string const Desc = "Description";
  auto const Sev = Severity::INFO;

  // Make a reporter with a logger sink
  auto TestLogSink = std::unique_ptr<Metrics::Sink>(
      new Metrics::MockSink(Metrics::LogTo::LOG_MSG));
  auto TestLogReporter = std::shared_ptr<Metrics::Reporter>(
      new Metrics::MockReporter(std::move(TestLogSink), 10ms));

  // Make a reporter with a Carbon sink
  auto TestCarbonSink = std::unique_ptr<Metrics::Sink>(
      new Metrics::MockSink(Metrics::LogTo::CARBON));
  auto TestCarbonReporter = std::shared_ptr<Metrics::Reporter>(
      new Metrics::MockReporter(std::move(TestCarbonSink), 10ms));

  std::vector<std::shared_ptr<Metrics::Reporter>> TestReporterList = {
      TestLogReporter, TestCarbonReporter};

  std::string const EmptyPrefix;
  auto TestRegistrar = Metrics::Registrar(EmptyPrefix, TestReporterList);
  auto TestLogReporterMock =
      std::dynamic_pointer_cast<MockReporter>(TestLogReporter);
  auto TestCarbonReporterMock =
      std::dynamic_pointer_cast<MockReporter>(TestCarbonReporter);

  REQUIRE_CALL(*TestCarbonReporterMock, addMetric(_, Name))
      .TIMES(1)
      .RETURN(true);
  REQUIRE_CALL(*TestLogReporterMock, addMetric(_, Name)).TIMES(1).RETURN(true);
  // Allow deregister call when Metric goes out of scope
  ALLOW_CALL(*TestCarbonReporterMock, tryRemoveMetric(Name)).RETURN(true);
  ALLOW_CALL(*TestLogReporterMock, tryRemoveMetric(Name)).RETURN(true);
  {
    Metric TestMetric(Name, Desc, Sev);
    TestRegistrar.registerMetric(TestMetric, {LogTo::CARBON, LogTo::LOG_MSG});
  }
}

} // namespace Metrics
