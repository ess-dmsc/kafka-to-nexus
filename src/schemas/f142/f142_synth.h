#pragma once
#include <flatbuffers/flatbuffers.h>
#include <memory>
#include <string>

namespace FlatBufs {
namespace f142 {

#include "schemas/f142_logdata_generated.h"

class fb {
public:
  std::unique_ptr<flatbuffers::FlatBufferBuilder> builder;
  LogData const *root();
};

class synth_impl;

class synth {
public:
  synth(std::string name, Value type);
  ~synth();
  fb next(uint64_t seq, size_t nele);
  std::unique_ptr<synth_impl> impl;
  std::string name;
};

} // namespace f142
} // namespace FlatBufs
