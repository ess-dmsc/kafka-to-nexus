#include <gtest/gtest.h>
#include "Metrics/Processor.h"
#include "ProcessorStandIn.h"
#include "MetricStandIn.h"
#include <trompeloeil.hpp>

namespace Metrics {

class MetricsRegistrarTest : public ::testing::Test {
public:
  ProcessorStandIn MockProcessor{};
};

using trompeloeil::_;
using trompeloeil::ne;
using std::string_literals::operator""s;

TEST_F(MetricsRegistrarTest, RegAndDeReg) {
  auto Name = "some_name"s;
  auto Desc = "Description"s;
  auto Sev = Severity::INFO;
  auto FullName = "some_name."s + Name;
  REQUIRE_CALL(MockProcessor, registerMetric(FullName, ne(nullptr), Desc, Sev, _)).TIMES(1).RETURN(true);
  REQUIRE_CALL(MockProcessor, deRegisterMetric(FullName)).TIMES(1).RETURN(true);
  {
    MetricStandIn Ctr(Name, Desc, Sev);
    auto Registrar = MockProcessor.getRegistrarBase();
    EXPECT_TRUE(Registrar.registerMetric(Ctr, {}));
  }
}


TEST_F(MetricsRegistrarTest, RegAndDeReg2) {
  auto BasePrefix = "some_name."s;
  auto ExtraPrefix = "some_prefix"s;
  auto Name = "some_metric"s;
  auto Desc = "Description"s;
  auto Sev = Severity::INFO;
  auto FullName = BasePrefix + ExtraPrefix + "."s + Name;
  auto Registrar1 = MockProcessor.getRegistrarBase();
  auto Registrar2 = Registrar1.getNewRegistrar(ExtraPrefix);

  REQUIRE_CALL(MockProcessor, registerMetric(FullName, ne(nullptr), Desc, Sev, _)).TIMES(1).RETURN(true);
  REQUIRE_CALL(MockProcessor, deRegisterMetric(FullName)).TIMES(1).RETURN(true);
  {
    MetricStandIn Ctr(Name, Desc, Sev);
    Registrar2.registerMetric(Ctr, {});
  }
}

TEST_F(MetricsRegistrarTest, RegisterEmptyNameFail) {
  auto EmptyName = ""s;
  auto Desc = "Description"s;
  auto Sev = Severity::INFO;
  FORBID_CALL(MockProcessor, registerMetric(_, _, _, _, _));
  FORBID_CALL(MockProcessor, deRegisterMetric(_));
  auto Registrar = MockProcessor.getRegistrarBase();
  {
    MetricStandIn Ctr(EmptyName, Desc, Sev);
    EXPECT_THROW(Registrar.registerMetric(Ctr, {}), std::runtime_error);
  }
}

TEST_F(MetricsRegistrarTest, RegisterNameFail) {
  auto Name = "yet_another_name"s;
  auto Desc = "Description"s;
  auto Sev = Severity::INFO;
  auto FullName = "some_name."s + Name;
  REQUIRE_CALL(MockProcessor, registerMetric(FullName, ne(nullptr), Desc, Sev, _)).TIMES(1).RETURN(false);
  FORBID_CALL(MockProcessor, deRegisterMetric(_));
  auto Registrar = MockProcessor.getRegistrarBase();
  {
    MetricStandIn Ctr(Name, Desc, Sev);
    EXPECT_FALSE(Registrar.registerMetric(Ctr, {}));
  }
}

} // namespace Metrics
