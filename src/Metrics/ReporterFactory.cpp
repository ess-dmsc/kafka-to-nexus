#include "Reporter.h"
#include "Registrar.h"
#include "Sink.h"
#include "Reporter.h"

namespace Metrics {

Reporter createReporter(std::shared_ptr<Registrar> const &MetricsRegistrar,
                        Metrics::LogTo SinkType) {
  auto ListOfMetrics = std::make_shared<MetricsList>();

  std::unique_ptr<Sink> MetricSink;
  switch (SinkType) {
  case LogTo::CARBON:
    MetricSink = std::unique_ptr<Sink>(new CarbonSink());
    break;
  case LogTo::LOG_MSG:
    MetricSink = std::unique_ptr<Sink>(new LogSink());
  }

  Reporter NewReporter(std::move(MetricSink), ListOfMetrics);
  MetricsRegistrar->addMetricsList(SinkType, ListOfMetrics);

  return NewReporter;
}

} // namespace Metrics
