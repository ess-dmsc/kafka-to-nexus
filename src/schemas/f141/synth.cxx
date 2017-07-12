#include "synth.h"
#include <random>

namespace FlatBufs {
namespace f141_epics_nt {

EpicsPV const *fb::root() { return GetEpicsPV(builder->GetBufferPointer()); }

synth::synth(std::string name, PV type, int size, uint64_t seed)
    : name(name), size(size) {
  impl.reset(new synth_impl);
  impl->type = type;
}

synth::~synth() {}

} // namespace f141_epics_nt
} // namespace FlatBufs
