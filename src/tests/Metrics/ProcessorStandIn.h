#pragma once

#include "Metrics/Processor.h"
#include "Metrics/Type.h"
#include "Metrics/Registrar.h"
#include <trompeloeil.hpp>

namespace Metrics {

class ProcessorStandIn : public Processor {
public:
  ProcessorStandIn() : Processor("some_name", "some_addr", 0) {};
  MAKE_MOCK5(registerMetric, bool(std::string, CounterType
      *, std::string, Severity, DestList), override);
  MAKE_MOCK1(deRegisterMetric, bool(std::string), override);
  MAKE_MOCK0(getRegistrar, Registrar(), override);
  Registrar getRegistrarBase() {return Processor::getRegistrar();}
  bool registerMetricBase(std::string Name, CounterType
  *Counter, std::string Description, Severity LogLevel, DestList Targets) { return Processor::registerMetric(Name, Counter, Description, LogLevel, Targets);}
  using Processor::LogMsgMetrics;
  using Processor::GrafanaMetrics;
};

} // namespace Metrics
