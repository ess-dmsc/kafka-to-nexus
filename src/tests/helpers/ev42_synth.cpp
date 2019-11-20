// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ev42_synth.h"
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

} // namespace ev42
} // namespace FlatBufs
