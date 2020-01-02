#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace Metrics {

class ProcessorInterface;
class Registrar;

enum class Severity {
  DEBUG,
  INFO,
  WARNING,
  ERROR
};

using CounterType = std::atomic_int64_t;
using InternalCounterType = decltype(((CounterType*)(nullptr))->load());

class Metric {
public:
  Metric(std::string Name, std::string Description, Severity Level = Severity::DEBUG) : MName(Name), MDesc(Description), SevLvl(Level) {}
  ~Metric();
  std::int64_t operator++() {Counter.store(Counter.load() + 1); return Counter.load();};
  std::int64_t operator++(int) {Counter.store(Counter.load() + 1); return Counter.load();};
  std::int64_t operator=(std::int64_t const &NewValue) {Counter.store(NewValue); return Counter.load();};
  std::int64_t operator+=(std::int64_t  AddValue) {Counter.store(AddValue + Counter.load()); return Counter.load();};
protected:
  friend Registrar;
  void setDeRegParams(std::string FullName, ProcessorInterface *Ptr) {DeRegName = FullName; DeRegPtr = Ptr;};
  std::string getName() {return MName;}
  std::string getDescription() {return MDesc;}
  Severity getSeverity() {return SevLvl;}
  CounterType* getCounterPtr() {return &Counter;}

  std::string MName;
  std::string DeRegName;
  std::string MDesc;
  Severity SevLvl;
  CounterType Counter{0};
  ProcessorInterface *DeRegPtr{nullptr};
};
}
