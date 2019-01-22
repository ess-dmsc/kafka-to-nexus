#pragma once

#include <flatbuffers/flatbuffers.h>
#include <memory>
#include <string>

namespace FlatBufs {
namespace f142 {

#include "f142_logdata_generated.h"

/// \brief Wrapper around the flatbuffer builder to facilitate the interface of
/// `synth`.
class fb {
public:
  std::unique_ptr<flatbuffers::FlatBufferBuilder> builder;
  LogData const *root();
};

/// Forward-declare the implementation.
class SynthImpl;

/// Simple test data generator for the f142 schema.
class synth {
public:
  synth(std::string SynthName, Value Type);
  ~synth();
  fb next(uint64_t seq, size_t nele);
  std::unique_ptr<SynthImpl> impl;
  std::string Name;
};

} // namespace f142
} // namespace FlatBufs
