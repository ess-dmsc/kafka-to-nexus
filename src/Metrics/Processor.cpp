#include "Processor.h"

namespace Metrics {

InternalMetric::InternalMetric(std::string Name, std::string Description, CounterType *Counter, Severity Lvl) : FullName(Name), Counter(Counter), DescriptionString(Description), LastValue(*Counter), ValueSeverity(Lvl) {

}

Processor::Processor(std::string AppName, std::string, std::uint16_t, PollInterval Log, PollInterval Carbon) : ProcessorInterface(), Prefix(AppName), LogMsgInterval(Log), CarbonInterval(Carbon) {

}

Registrar Processor::getRegistrar() {
  return {Prefix, dynamic_cast<ProcessorInterface *>(this)};
}

bool Processor::registerMetric(std::string Name, Metrics::CounterType * Counter, std::string Description, Severity LogLevel, Metrics::DestList Targets) {
  InternalMetric MetricToAdd(Name, Description, Counter, LogLevel);
  auto IsInLogsList = std::any_of(LogMsgMetrics.begin(), LogMsgMetrics.end(), [&Name](auto const &Elem){
    return Elem.FullName == Name;
  });
  auto IsInGrafanaList = std::any_of(GrafanaMetrics.begin(), GrafanaMetrics.end(), [&Name](auto const &Elem){
    return Elem.FullName == Name;
  });
  if (IsInGrafanaList or IsInLogsList) {
    return false;
  }
  std::lock_guard<std::mutex> LocalLock(MetricsMutex);
  if (Targets.find(LogTo::GRAPHITE) != Targets.end()) {
    GrafanaMetrics.push_back(MetricToAdd);
  }
  if (Targets.find(LogTo::LOG_MSG) != Targets.end()) {
    LogMsgMetrics.push_back(MetricToAdd);
  }

  return true;
}

} // namespace Metrics
