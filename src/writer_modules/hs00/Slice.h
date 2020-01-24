// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "json.h"
#include <vector>

namespace WriterModule {
namespace hs00 {

class Slice {
public:
  static Slice fromOffsetsSizes(std::vector<uint32_t> const &SliceOffsets,
                                std::vector<uint32_t> const &SliceSizes);

  bool doesOverlap(Slice const &Other) const;

private:
  std::vector<uint32_t> Offsets;
  std::vector<uint32_t> Sizes;
};
} // namespace hs00
} // namespace WriterModule
