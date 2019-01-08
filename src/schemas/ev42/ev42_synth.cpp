#include "ev42_synth.h"
#include "../../logger.h"
#include <random>

namespace FlatBufs {
namespace ev42 {

EventMessage const *FlatBufferWrapper::root() {
  return GetEventMessage(builder->GetBufferPointer());
}

synth::synth(std::string SynthName, uint64_t seed)
    : Name(std::move(SynthName)) {
  impl = std::make_unique<synth_impl>();
  impl->RandomNumberGen.seed(seed);
}

FlatBufferWrapper synth::next(uint32_t size) {
  using DT = uint32_t;
  FlatBufferWrapper ret;
  ret.builder = std::make_unique<flatbuffers::FlatBufferBuilder>(size * 4 * 2 + 1024);
  auto n = ret.builder->CreateString(Name);
  DT *BufferPtr1 = nullptr;
  auto v1 =
      ret.builder->CreateUninitializedVector(size, sizeof(DT), reinterpret_cast<uint8_t **>(&BufferPtr1));
  DT *BufferPtr2 = nullptr;
  auto v2 =
      ret.builder->CreateUninitializedVector(size, sizeof(DT), reinterpret_cast<uint8_t **>(&BufferPtr2));

  if ((BufferPtr1 == nullptr) or (BufferPtr2 == nullptr)) {
    LOG(Sev::Debug, "Failed to create test data");
  } else {
    for (size_t i1 = 0; i1 < size; ++i1) {
      auto eid = impl->RandomNumberGen();
      BufferPtr1[i1] = eid & 0xff;
      BufferPtr2[i1] = (eid >> 8) & 0xff;
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
