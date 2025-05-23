#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace Metrics {

class Reporter;

enum struct Severity { DEBUG, INFO, WARNING, ERROR };

using CounterType = std::atomic<int64_t>;

/// \brief Metrics counter class.
///
/// The metric is stored as int64.
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

  template <typename CType> bool operator==(CType const &Rhs) const {
    if constexpr (std::is_same_v<CType, std::nullptr_t>) {
      return false;
    } else {
      return Counter.load(MemoryOrder) == Rhs;
    }
  };

  template <typename CType> explicit operator CType() const {
    return static_cast<CType>(Counter.load());
  };

  int64_t operator=(int64_t const &NewValue) {
    Counter.store(NewValue, MemoryOrder);
    return Counter.load(MemoryOrder);
  };
  std::string operator=(std::string NewValue) {
    Value = NewValue;
    return Value;
  };
  int64_t operator+=(int64_t AddValue) {
    Counter.store(AddValue + Counter.load(MemoryOrder), MemoryOrder);
    return Counter.load(MemoryOrder);
  };

  std::string getName() const { return MName; }
  std::string getDescription() const { return MDesc; }
  Severity getSeverity() const { return SevLvl; }
  CounterType *getCounterPtr() { return &Counter; }
  std::string *getStringPtr();

  void setDeregistrationDetails(std::string const &NameWithPrefix,
                                std::shared_ptr<Reporter> const &Reporter);

private:
  // Details used for deregistration, keeping these rather than the Registrar
  // means that the Registrar does not need to be kept alive until the metric is
  // deregistered
  std::string FullName;
  std::vector<std::shared_ptr<Reporter>> Reporters;

  std::memory_order const MemoryOrder{std::memory_order::memory_order_relaxed};
  std::string const MName;
  std::string const MDesc;
  Severity const SevLvl;
  CounterType Counter{0};
	std::string Value = "";
  std::string CounterStr;
};

} // namespace Metrics
