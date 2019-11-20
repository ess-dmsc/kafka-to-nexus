// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "f142_synth.h"
#include <random>

namespace FlatBufs {
namespace f142 {

LogData const *FlatBufferWrapper::root() {
  return GetLogData(builder->GetBufferPointer());
}

Synth::Synth(std::string SynthName, Value Type)
    : Name(std::move(SynthName)), Impl(std::make_unique<SynthImpl>()) {
  Impl->Type = Type;
}

} // namespace f142
} // namespace FlatBufs
