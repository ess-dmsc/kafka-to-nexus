#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace Metrics {

class ProcessorInterface;
class Registrar;

enum class Severity { DEBUG, INFO, WARNING, ERROR };

using CounterType = std::atomic<std::int64_t>;
using InternalCounterType = decltype(((CounterType *)(nullptr))->load());

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
  int64_t operator=(int64_t const &NewValue) {
    Counter.store(NewValue, MemoryOrder);
    return Counter.load(MemoryOrder);
  };
  int64_t operator+=(int64_t AddValue) {
    Counter.store(AddValue + Counter.load(MemoryOrder), MemoryOrder);
    return Counter.load(MemoryOrder);
  };

protected:
  friend Registrar;
  void setDeRegParams(std::string FullName, ProcessorInterface *Ptr) {
    DeRegName = std::move(FullName);
    DeRegPtr = Ptr;
  };
  std::string getName() const { return MName; }
  std::string getDescription() const { return MDesc; }
  Severity getSeverity() const { return SevLvl; }
  CounterType *getCounterPtr() { return &Counter; }

  std::string MName;
  std::string DeRegName;
  std::string MDesc;
  Severity SevLvl;
  CounterType Counter{0};
  ProcessorInterface *DeRegPtr{nullptr};

private:
  std::memory_order const MemoryOrder{std::memory_order::memory_order_relaxed};
};
} // namespace Metrics
