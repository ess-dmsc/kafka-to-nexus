#pragma once

#include "json.h"
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

class Slice {
public:
  static Slice fromOffsetsSizes(std::vector<uint32_t> Offsets_,
                                std::vector<uint32_t> Sizes_);

  bool doesOverlap(Slice const &Other);

private:
  std::vector<uint32_t> Offsets;
  std::vector<uint32_t> Sizes;
};
}
}
}
