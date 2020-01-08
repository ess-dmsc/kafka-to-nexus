#include "CarbonSink.h"
#include "LogSink.h"
#include "Registrar.h"
#include "Reporter.h"
#include "Sink.h"

namespace Metrics {

Reporter createReporter(std::shared_ptr<Registrar> const &MetricsRegistrar,
                        Metrics::LogTo SinkType,
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

  Reporter NewReporter(std::move(MetricSink), ListOfMetrics);
  MetricsRegistrar->addMetricsList(SinkType, ListOfMetrics);

  return NewReporter;
}

} // namespace Metrics
