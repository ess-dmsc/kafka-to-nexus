#include "Registrar.h"
#include "Processor.h"

namespace Metrics {

Registrar::Registrar(std::string MetricsPrefix, ProcessorInterface *ProcessorPtr)
    : Prefix(MetricsPrefix), MetricsProcessor(ProcessorPtr) {}

bool Registrar::registerMetric(Metric &NewMetric, DestList Destinations) {
  if (NewMetric.getName().empty()) {
    throw std::runtime_error("Can not register a metric with an empty name.");
  }
  auto NewMetricString = Prefix + "." + NewMetric.getName();
  if (MetricsProcessor->registerMetric(NewMetricString, NewMetric.getCounterPtr(),
                                NewMetric.getDescription(),
                                NewMetric.getSeverity(), Destinations)) {
    NewMetric.setDeRegParams(NewMetricString, MetricsProcessor);
    return true;
  }
  return false;
}

Registrar Registrar::getNewRegistrar(std::string MetricsPrefix) {
  return {Prefix + "." + MetricsPrefix, MetricsProcessor};
}

} // namespace Metrics
