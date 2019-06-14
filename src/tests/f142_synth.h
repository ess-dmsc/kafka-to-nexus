#pragma once

#include <flatbuffers/flatbuffers.h>
#include <memory>
#include <string>

namespace FileWriter {
namespace Schemas {
namespace f142 {

#include "f142_logdata_generated.h"

/// \brief Wrapper around the flatbuffer builder to facilitate the interface of
/// `synth`.
class FlatBufferWrapper {
public:
  std::unique_ptr<flatbuffers::FlatBufferBuilder> builder;
  LogData const *root();
};

/// \brief Used to generate test data.
struct SynthImpl {
  Value Type;
};

/// Simple test data generator for the f142 schema.
class Synth {
public:
  Synth(std::string SynthName, Value Type);
  FlatBufferWrapper next(uint64_t TestValue, size_t NrOfElements);
  std::string Name;
  std::unique_ptr<SynthImpl> Impl;
};

} // namespace f142
} // namespace Schemas
} // namespace FileWriter
