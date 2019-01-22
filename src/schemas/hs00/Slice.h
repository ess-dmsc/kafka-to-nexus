#pragma once

#include "json.h"
#include <vector>

namespace FileWriter {
namespace Schemas {
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
} // namespace Schemas
} // namespace FileWriter
