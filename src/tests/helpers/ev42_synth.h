// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include "ev42_events_generated.h"
#include "logger.h"
#include <memory>
#include <random>
#include <string>

namespace FlatBufs {
namespace ev42 {

class FlatBufferWrapper {
public:
  FlatBufferWrapper() = default;
  FlatBufferWrapper(FlatBufferWrapper &&x) noexcept {
    std::swap(Builder, x.Builder);
  }
  std::unique_ptr<flatbuffers::FlatBufferBuilder> Builder;
  EventMessage const *root();
};

/// \brief Used for generating test data.
struct SynthImpl {
  std::mt19937 RandomNumberGen;
  uint64_t seq = 0;
  uint64_t c1 = 0;
};

class Synth {
public:
  Synth(std::string SynthName, uint64_t Seed);
  FlatBufferWrapper next(uint32_t Size);
  std::string Name;
  std::unique_ptr<SynthImpl> Impl;

private:
  SharedLogger Logger = getLogger();
};

} // namespace ev42
} // namespace FlatBufs
