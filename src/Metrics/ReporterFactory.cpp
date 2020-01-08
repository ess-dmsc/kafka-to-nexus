#include "CarbonSink.h"
#include "LogSink.h"
#include "Registrar.h"
#include "Reporter.h"
#include "Sink.h"

namespace Metrics {

std::unique_ptr<Reporter>
createReporter(std::shared_ptr<Registrar> const &MetricsRegistrar,
               Metrics::LogTo SinkType, std::chrono::milliseconds Interval,
               std::string const &CarbonHost = "",
               uint16_t const CarbonPort = 0) {
  auto ListOfMetrics = std::make_shared<MetricsList>();

  std::unique_ptr<Sink> MetricSink;
  switch (SinkType) {
  case LogTo::CARBON:
    MetricSink = std::unique_ptr<Sink>(new CarbonSink(CarbonHost, CarbonPort));
    break;
  case LogTo::LOG_MSG:
    MetricSink = std::unique_ptr<Sink>(new LogSink());
  }

  MetricsRegistrar->addMetricsList(SinkType, ListOfMetrics);

  return std::make_unique<Reporter>(std::move(MetricSink), ListOfMetrics,
                                    Interval);
}

} // namespace Metrics
