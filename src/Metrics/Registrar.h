#pragma once

#include "Reporter.h"
#include "Sink.h"
#include <map>
#include <string>
#include <vector>

namespace Metrics {

class Metric;
class Registrar;

class IRegistrar {
public:
  virtual void registerMetric(Metric &NewMetric,
                              std::vector<LogTo> const &SinkTypes) const = 0;
  [[nodiscard]] virtual std::unique_ptr<IRegistrar>
  getNewRegistrar(std::string const &MetricsPrefix) const = 0;
  virtual ~IRegistrar() = default;
};

/// Register and metrics to be reported via a specified sink
/// Manages metrics name prefixes
/// Deregistration of metric happens via Metric's destructor
class Registrar : public IRegistrar {
public:
  explicit Registrar(
      std::string MetricsPrefix,
      std::vector<std::shared_ptr<Reporter>> const &Reporters = {})
      : Prefix(std::move(MetricsPrefix)), ReporterList(Reporters){};
  ~Registrar() override = default;

  void registerMetric(Metric &NewMetric,
                      std::vector<LogTo> const &SinkTypes) const override;

  [[nodiscard]] std::unique_ptr<IRegistrar>
  getNewRegistrar(std::string const &MetricsPrefix) const override;

private:
  [[nodiscard]] std::string prependPrefix(std::string const &Name) const;
  std::string Prefix;
  /// List of reporters we might want to add a metric to
  std::vector<std::shared_ptr<Reporter>> ReporterList;
};

} // namespace Metrics
