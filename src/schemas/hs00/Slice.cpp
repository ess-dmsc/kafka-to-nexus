#include "Slice.h"
#include "Exceptions.h"
#include "Writer.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

Slice Slice::fromOffsetsSizes(std::vector<uint32_t> Offsets_,
                              std::vector<uint32_t> Sizes_) {
  Slice TheSlice;
  TheSlice.Offsets = Offsets_;
  TheSlice.Sizes = Sizes_;
  return TheSlice;
}
}
}
}
