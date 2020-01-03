#include "Processor.h"

namespace Metrics {

InternalMetric::InternalMetric(std::string Name, std::string Description,
                               CounterType *Counter, Severity Lvl)
    : FullName(std::move(Name)), Counter(Counter),
      DescriptionString(std::move(Description)), LastValue(*Counter),
      ValueSeverity(Lvl) {}

Processor::Processor(std::string AppName, std::string CarbonAddress,
                     std::uint16_t CarbonPort, PollInterval Log,
                     PollInterval Carbon)
    : ProcessorInterface(), Prefix(std::move(AppName)),
      LogMsgInterval(std::move(Log)), CarbonInterval(std::move(Carbon)),
      MetricsThread(&Processor::threadFunction, this),
      Carbon(std::move(CarbonAddress), CarbonPort) {}

Processor::~Processor() {
  RunThread.store(false);
  if (MetricsThread.joinable()) {
    MetricsThread.join();
  }
}

Registrar Processor::getRegistrar() {
  return {Prefix, dynamic_cast<ProcessorInterface *>(this)};
}

bool Processor::registerMetric(std::string Name, Metrics::CounterType *Counter,
                               std::string Description, Severity LogLevel,
                               Metrics::DestList Targets) {
  if (metricIsInList(Name)) {
    Logger->warn("Unable to register metric with name \"{}\" as it is already "
                 "registered.",
                 Name);
    return false;
  }
  std::lock_guard<std::mutex> LocalLock(MetricsMutex);
  InternalMetric MetricToAdd(Name, Description, Counter, LogLevel);
  if (Targets.find(LogTo::CARBON) != Targets.end()) {
    GrafanaMetrics.push_back(MetricToAdd);
    Logger->info("Registering the metric \"{}\" for publishing to Grafana.",
                 Name);
  }
  if (Targets.find(LogTo::LOG_MSG) != Targets.end()) {
    LogMsgMetrics.push_back(MetricToAdd);
    Logger->info("Registering the metric \"{}\" for printing as a log message.",
                 Name);
  }
  return true;
}

bool Processor::metricIsInList(std::string const &Name) {
  std::lock_guard<std::mutex> LocalLock(MetricsMutex);
  auto IsInLogsList =
      std::any_of(LogMsgMetrics.begin(), LogMsgMetrics.end(),
                  [&Name](auto const &Elem) { return Elem.FullName == Name; });
  auto IsInGrafanaList =
      std::any_of(GrafanaMetrics.begin(), GrafanaMetrics.end(),
                  [&Name](auto const &Elem) { return Elem.FullName == Name; });
  return IsInLogsList or IsInGrafanaList;
}

bool Processor::deRegisterMetric(std::string Name) {
  if (not metricIsInList(Name)) {
    Logger->warn(
        "Unable to de-register the metric \"{}\" as it is not a known metric.",
        Name);
    return false;
  }
  std::lock_guard<std::mutex> LocalLock(MetricsMutex);
  auto CheckMetricName = [&Name](auto const &Element) {
    return Name == Element.FullName;
  };
  Logger->info("De-registering the metric \"{}\".", Name);
  LogMsgMetrics.erase(std::remove_if(LogMsgMetrics.begin(), LogMsgMetrics.end(),
                                     CheckMetricName),
                      LogMsgMetrics.end());
  GrafanaMetrics.erase(std::remove_if(GrafanaMetrics.begin(),
                                      GrafanaMetrics.end(), CheckMetricName),
                       GrafanaMetrics.end());
  return true;
}

void Processor::threadFunction() {
  auto Now = std::chrono::system_clock::now();
  auto NextGrafanaTime = Now + CarbonInterval;
  auto NextLogMsgTime = Now + LogMsgInterval;
  auto NextExitThreadCheck = Now + ExitThreadCheckInterval;
  auto findNextStopTime = [](auto const &Now, auto const &PreviousStopTime,
                             auto const &UsedInterval) {
    if (Now < PreviousStopTime) {
      return PreviousStopTime;
    }
    auto IntervalMultiplier = (Now - PreviousStopTime) / UsedInterval;
    return PreviousStopTime + (IntervalMultiplier + 1) * UsedInterval;
  };
  while (RunThread) {
    auto NextWakeTime = std::min(std::min(NextGrafanaTime, NextLogMsgTime),
                                 NextExitThreadCheck);
    std::this_thread::sleep_until(NextWakeTime);
    Now = std::chrono::system_clock::now();
    if (Now > NextGrafanaTime) {
      generateGrafanaUpdate();
    }
    if (Now > NextLogMsgTime) {
      generateLogMessages();
    }

    NextGrafanaTime = findNextStopTime(Now, NextGrafanaTime, CarbonInterval);
    NextLogMsgTime = findNextStopTime(Now, NextLogMsgTime, LogMsgInterval);
    NextExitThreadCheck =
        findNextStopTime(Now, NextExitThreadCheck, ExitThreadCheckInterval);
  }
}

void Processor::generateLogMessages() {
  auto Now = std::chrono::system_clock::now();
  std::lock_guard<std::mutex> LocalLock(MetricsMutex);
  for (auto &CMetric : LogMsgMetrics) {
    auto CurrentValue = CMetric.Counter->load();
    if (CurrentValue != CMetric.LastValue) {
      auto ValueDiff = CurrentValue - CMetric.LastValue;
      CMetric.LastValue = CurrentValue;
      auto TimeDiff = std::chrono::duration_cast<std::chrono::milliseconds>(
                          Now - CMetric.LastTime)
                          .count();
      Logger->log(
          LogSeverityMap[CMetric.ValueSeverity],
          "In the past {} ms, {} events of type \"{}\" have occured ({}).",
          TimeDiff, ValueDiff, CMetric.FullName, CMetric.DescriptionString);
    }
    CMetric.LastTime = Now;
  }
}

void Processor::generateGrafanaUpdate() {
  if (not Carbon.messageQueueEmpty()) {
    return;
  }
  auto Now = std::chrono::system_clock::now();
  std::lock_guard<std::mutex> LocalLock(MetricsMutex);
  for (auto &CMetric : GrafanaMetrics) {
    auto CurrentValue = CMetric.Counter->load();
    auto CurrentName = CMetric.FullName;
    sendMsgToCarbon(CurrentName, CurrentValue, Now);
  }
}

void Processor::sendMsgToCarbon(
    std::string Name, Metrics::InternalCounterType Value,
    std::chrono::system_clock::time_point ValueTime) {
  auto TimeSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(
                            ValueTime.time_since_epoch())
                            .count();
  Carbon.sendMessage(fmt::format("{} {} {}\n", Name, Value, TimeSinceEpoch));
}

} // namespace Metrics
