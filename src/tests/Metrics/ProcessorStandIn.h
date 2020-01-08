#pragma once

#include "Metrics/Processor.h"
#include "Metrics/Registrar.h"
#include "Metrics/Type.h"
#include <trompeloeil.hpp>

namespace Metrics {

class ProcessorStandIn : public Processor {
public:
  ProcessorStandIn(PollInterval LogMsg = 100ms, PollInterval CarbonUpdt = 500ms)
      : Processor("some_name", "some_addr", 0, LogMsg, CarbonUpdt){};
  MAKE_MOCK5(registerMetric,
             bool(std::string, CounterType *, std::string, Severity, DestList),
             override);
  MAKE_MOCK1(deregisterMetric, bool(std::string), override);
  MAKE_MOCK0(getRegistrar, Registrar(), override);
  Registrar getRegistrarBase() { return Processor::getRegistrar(); }
  bool registerMetricBase(std::string Name, CounterType *Counter,
                          std::string Description, Severity LogLevel,
                          DestList Targets) {
    return Processor::registerMetric(Name, Counter, Description, LogLevel,
                                     Targets);
  }
  bool deRegisterMetricBase(std::string Name) {
    return Processor::deregisterMetric(Name);
  }
  MAKE_MOCK3(sendMsgToCarbon, void(std::string, InternalCounterType,
                                   std::chrono::system_clock::time_point),
             override);
  void sendMsgToCarbonBase(std::string Name, InternalCounterType Value,
                           std::chrono::system_clock::time_point Time) {
    Processor::sendMsgToCarbon(Name, Value, Time);
  };
  using Processor::GrafanaMetrics;
  using Processor::Logger;
  using Processor::LogMsgMetrics;
};

} // namespace Metrics
