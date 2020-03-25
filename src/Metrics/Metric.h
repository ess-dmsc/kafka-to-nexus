#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

namespace Metrics {

class Reporter;

enum struct Severity { DEBUG, INFO, WARNING, ERROR };

using CounterType = std::atomic<int64_t>;

class Metric {
public:
  Metric(std::string Name, std::string Description,
         Severity Level = Severity::DEBUG)
      : MName(std::move(Name)), MDesc(std::move(Description)), SevLvl(Level) {}
  ~Metric();
  int64_t operator++() {
    Counter.store(Counter.load(MemoryOrder) + 1, MemoryOrder);
    return Counter.load(MemoryOrder);
  };
  int64_t operator++(int) {
    Counter.store(Counter.load(MemoryOrder) + 1, MemoryOrder);
    return Counter.load(MemoryOrder);
  };

  template <typename CType> bool operator==(CType const &Rhs) {
    return Counter.load(MemoryOrder) == Rhs;
  };

  template <typename CType> explicit operator CType() const {
    return static_cast<CType>(Counter.load());
  }

  int64_t operator=(int64_t const &NewValue) {
    Counter.store(NewValue, MemoryOrder);
    return Counter.load(MemoryOrder);
  };
  int64_t operator+=(int64_t AddValue) {
    Counter.store(AddValue + Counter.load(MemoryOrder), MemoryOrder);
    return Counter.load(MemoryOrder);
  };

  std::string getName() const { return MName; }
  std::string getDescription() const { return MDesc; }
  Severity getSeverity() const { return SevLvl; }
  CounterType *getCounterPtr() { return &Counter; }

  void setDeregistrationDetails(
      std::string const &NameWithPrefix,
      std::shared_ptr<Reporter> const &ReporterResponsibleForMetric) {
    FullName = NameWithPrefix;
    ReporterForMetric = ReporterResponsibleForMetric;
  }

private:
  // Details used for deregistration, keeping these rather than the Registrar
  // means that the Registrar does not need to be kept alive until the metric is
  // deregistered
  std::string FullName;
  std::shared_ptr<Reporter> ReporterForMetric;

  std::memory_order const MemoryOrder{std::memory_order::memory_order_relaxed};
  std::string const MName;
  std::string const MDesc;
  Severity const SevLvl;
  CounterType Counter{0};
};
} // namespace Metrics
