#include "ev42_synth.h"
#include "../../logger.h"
#include <random>

namespace FlatBufs {
namespace ev42 {

EventMessage const *fb::root() {
  return GetEventMessage(builder->GetBufferPointer());
}

class synth_impl {
  friend class synth;
  std::mt19937 rnd;
  uint64_t seq = 0;
  uint64_t c1 = 0;
};

synth::synth(std::string name, uint64_t seed) : name(name) {
  impl.reset(new synth_impl);
  impl->rnd.seed(seed);
}

synth::~synth() {}

fb synth::next(uint32_t size) {
  using DT = uint32_t;
  fb ret;
  ret.builder.reset(new flatbuffers::FlatBufferBuilder(size * 4 * 2 + 1024));
  auto n = ret.builder->CreateString(name);
  DT *a1 = nullptr;
  auto v1 =
      ret.builder->CreateUninitializedVector(size, sizeof(DT), (uint8_t **)&a1);
  DT *a2 = nullptr;
  auto v2 =
      ret.builder->CreateUninitializedVector(size, sizeof(DT), (uint8_t **)&a2);

  if ((!a1) || (!a2)) {
    LOG(Sev::Debug, "Failed to create test data");
  } else {
    for (size_t i1 = 0; i1 < size; ++i1) {
      auto eid = impl->rnd();
      a1[i1] = eid & 0xff;
      a2[i1] = (eid >> 8) & 0xff;
      impl->c1 += 1;
    }
  }

  EventMessageBuilder b1(*ret.builder);
  b1.add_message_id(impl->seq);
  b1.add_source_name(n);
  b1.add_pulse_time(100 * impl->seq);
  b1.add_time_of_flight(v1);
  b1.add_detector_id(v2);
  FinishEventMessageBuffer(*ret.builder, b1.Finish());
  ++impl->seq;
  return ret;
}

} // namespace ev42
} // namespace FlatBufs
