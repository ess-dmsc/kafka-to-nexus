
#pragma once

#include "Registrar.h"
#include "CarbonInterface.h"
#include "Type.h"
#include "logger.h"
#include <chrono>
#include <map>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

namespace Metrics {

struct InternalMetric {
  InternalMetric(std::string Name, std::string Description,
                 CounterType *Counter, Severity Lvl);
  std::string FullName;
  CounterType *Counter{nullptr};
  std::string DescriptionString;
  std::int64_t LastValue{0};
  std::chrono::system_clock::time_point LastTime{
      std::chrono::system_clock::now()};
  Severity ValueSeverity{Severity::ERROR};
};

class ProcessorInterface {
public:
  virtual ~ProcessorInterface() = default;

  virtual bool registerMetric(std::string Name, CounterType *Counter,
                              std::string Description, Severity LogLevel,
                              DestList Targets) = 0;

  virtual bool deRegisterMetric(std::string Name) = 0;

  virtual Registrar getRegistrar() = 0;
};

using PollInterval = std::chrono::system_clock::duration;
using namespace std::chrono_literals;

class Processor : public ProcessorInterface {
public:
  Processor(std::string AppName, std::string CarbonAddress,
            std::uint16_t CarbonPort, PollInterval Log = 100ms,
            PollInterval Carbon = 1s);

  virtual ~Processor();

  bool registerMetric(std::string Name, CounterType *Counter,
                      std::string Description, Severity LogLevel,
                      DestList Targets) override;

  bool deRegisterMetric(std::string) override;

  Registrar getRegistrar() override;

protected:
  bool metricIsInList(std::string Name);

  void threadFunction();

  void generateGrafanaUpdate();

  void generateLogMessages();

  virtual void sendMsgToCarbon(std::string Name,
                               Metrics::InternalCounterType Value,
                               std::chrono::system_clock::time_point ValueTime);

  std::string Prefix;
  PollInterval LogMsgInterval;
  PollInterval CarbonInterval;
  PollInterval const ExitThreadCheckInterval{20ms};
  std::vector<InternalMetric> LogMsgMetrics;
  std::vector<InternalMetric> GrafanaMetrics;
  SharedLogger Logger = getLogger();
  std::mutex MetricsMutex;
  std::atomic_bool RunThread{true};
  std::thread MetricsThread;
  CarbonConnection Carbon;
  using spdlog_lvl = spdlog::level::level_enum;
  std::unordered_map<Severity, spdlog::level::level_enum> LogSeverityMap{
      {Severity::DEBUG, spdlog_lvl::debug},
      {Severity::INFO, spdlog_lvl::info},
      {Severity::WARNING, spdlog_lvl::warn},
      {Severity::ERROR, spdlog_lvl::err}};
};

} // namespace Metrics
