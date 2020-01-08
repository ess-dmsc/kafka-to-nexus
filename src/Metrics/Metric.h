#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

namespace Metrics {

class Registrar;

enum struct Severity { DEBUG, INFO, WARNING, ERROR };

using CounterType = std::atomic<std::int64_t>;

class Metric {
public:
  Metric(std::shared_ptr<Registrar> RegistrarUsed, std::string Name,
         std::string Description, Severity Level = Severity::DEBUG)
      : MetricsRegistrar(std::move(RegistrarUsed)), MName(std::move(Name)),
        MDesc(std::move(Description)), SevLvl(Level) {}
  ~Metric();
  int64_t operator++() {
    Counter.store(Counter.load(MemoryOrder) + 1, MemoryOrder);
    return Counter.load(MemoryOrder);
  };
  int64_t operator++(int) {
    Counter.store(Counter.load(MemoryOrder) + 1, MemoryOrder);
    return Counter.load(MemoryOrder);
  };
  int64_t operator=(int64_t const &NewValue) {
    Counter.store(NewValue, MemoryOrder);
    return Counter.load(MemoryOrder);
  };
  int64_t operator+=(int64_t AddValue) {
    Counter.store(AddValue + Counter.load(MemoryOrder), MemoryOrder);
    return Counter.load(MemoryOrder);
  };

  std::string getName() const { return MName; }

private:
  std::shared_ptr<Registrar> MetricsRegistrar;
  std::memory_order const MemoryOrder{std::memory_order::memory_order_relaxed};

protected:
  std::string getDescription() const { return MDesc; }
  Severity getSeverity() const { return SevLvl; }
  CounterType *getCounterPtr() { return &Counter; }

  std::string const MName;
  std::string const MDesc;
  Severity const SevLvl;
  CounterType Counter{0};
};
} // namespace Metrics
