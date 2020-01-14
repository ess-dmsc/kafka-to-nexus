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
    TestRegistrarExtraPrefix.registerMetric(Ctr, {LogTo::LOG_MSG});
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

// TEST_F(MetricsRegistrarTest, RegisterNameFail) {
//  auto Name = std::string("yet_another_name");
//  auto Desc = std::string("Description");
//  auto Sev = Severity::INFO;
//  auto FullName = std::string("some_name.") + Name;
//  REQUIRE_CALL(MockProcessor,
//               registerMetric(FullName, ne(nullptr), Desc, Sev, _))
//      .TIMES(1)
//      .RETURN(false);
//  FORBID_CALL(MockProcessor, deregisterMetric(_));
//  auto Registrar = MockProcessor.getRegistrarBase();
//  {
//    Metric Ctr(Name, Desc, Sev);
//    EXPECT_FALSE(Registrar.registerMetric(Ctr, {}));
//  }
//}

// RegisterWithExistingNameFails?

} // namespace Metrics
