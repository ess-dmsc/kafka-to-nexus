#pragma once
#include "ev42_events_generated.h"
#include <memory>
#include <string>
#include <random>

namespace FlatBufs {
namespace ev42 {

class FlatBufferWrapper {
public:
  FlatBufferWrapper() {}
  FlatBufferWrapper(FlatBufferWrapper &&x) { std::swap(builder, x.builder); }
  std::unique_ptr<flatbuffers::FlatBufferBuilder> builder;
  EventMessage const *root();
};

struct synth_impl {
  std::mt19937 RandomNumberGen;
  uint64_t seq = 0;
  uint64_t c1 = 0;
};

class synth {
public:
  synth(std::string SynthName, uint64_t seed);
  FlatBufferWrapper next(uint32_t size);
  std::unique_ptr<synth_impl> impl;
  std::string Name;
};

} // namespace ev42
} // namespace FlatBufs
