#pragma once
#include "schemas/ev42_events_generated.h"
#include <memory>
#include <string>

namespace FlatBufs {
namespace ev42 {

class fb {
public:
  fb() {}
  fb(fb &&x) { std::swap(builder, x.builder); }
  std::unique_ptr<flatbuffers::FlatBufferBuilder> builder;
  EventMessage const *root();
};

class synth_impl;

class synth {
public:
  synth(std::string SynthName, uint64_t seed);
  ~synth();
  fb next(uint32_t size);
  std::unique_ptr<synth_impl> impl;
  std::string Name;
};

} // namespace ev42
} // namespace FlatBufs
