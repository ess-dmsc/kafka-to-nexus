#include "tests/ev42_synth.h"
#include "logger.h"
#include <random>

namespace FlatBufs {
namespace ev42 {

EventMessage const *FlatBufferWrapper::root() {
  return GetEventMessage(Builder->GetBufferPointer());
}

Synth::Synth(std::string SynthName, uint64_t Seed)
    : Name(std::move(SynthName)), Impl(std::make_unique<SynthImpl>()) {
  Impl->RandomNumberGen.seed(Seed);
}

FlatBufferWrapper Synth::next(uint32_t Size) {
  using DT = uint32_t;
  FlatBufferWrapper ret;
  ret.Builder =
      std::make_unique<flatbuffers::FlatBufferBuilder>(Size * 4 * 2 + 1024);
  auto n = ret.Builder->CreateString(Name);
  DT *BufferPtr1 = nullptr;
  auto v1 = ret.Builder->CreateUninitializedVector(
      Size, sizeof(DT), reinterpret_cast<uint8_t **>(&BufferPtr1));
  DT *BufferPtr2 = nullptr;
  auto v2 = ret.Builder->CreateUninitializedVector(
      Size, sizeof(DT), reinterpret_cast<uint8_t **>(&BufferPtr2));

  if ((BufferPtr1 == nullptr) or (BufferPtr2 == nullptr)) {
    Logger->trace("Failed to create test data");
  } else {
    for (size_t i1 = 0; i1 < Size; ++i1) {
      auto eid = Impl->RandomNumberGen();
      BufferPtr1[i1] = eid & 0xff;
      BufferPtr2[i1] = (eid >> 8) & 0xff;
      Impl->c1 += 1;
    }
  }

  EventMessageBuilder b1(*ret.Builder);
  b1.add_message_id(Impl->seq);
  b1.add_source_name(n);
  b1.add_pulse_time(100 * Impl->seq);
  b1.add_time_of_flight(v1);
  b1.add_detector_id(v2);
  FinishEventMessageBuffer(*ret.Builder, b1.Finish());
  ++Impl->seq;
  return ret;
}

} // namespace ev42
} // namespace FlatBufs
