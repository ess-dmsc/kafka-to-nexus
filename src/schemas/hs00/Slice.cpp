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

void Slice::printOverlap(Slice const &This, Slice const &Other) {
  for (size_t i = 0; i < This.Offsets.size(); ++i) {
    printf("%ld:  %d  %d  vs %d  %d\n", i, This.Offsets.at(i), This.Sizes.at(i),
           Other.Offsets.at(i), Other.Sizes.at(i));
  }
}

bool Slice::doesOverlap(Slice const &Other) {
  auto &This = *this;
  if (Offsets.size() != Sizes.size()) {
    throw std::runtime_error("Incompatible Slice shape");
  }
  if (Other.Offsets.size() != This.Offsets.size()) {
    throw std::runtime_error("Incompatible Slice shape");
  }
  if (Other.Sizes.size() != This.Sizes.size()) {
    throw std::runtime_error("Incompatible Slice shape");
  }
  size_t OverlappingDims = 0;
  for (size_t i = 0; i < Offsets.size(); ++i) {
    if (Other.Offsets.at(i) <= This.Offsets.at(i)) {
      if (Other.Offsets.at(i) + Other.Sizes.at(i) > This.Offsets.at(i)) {
        ++OverlappingDims;
      }
    } else if (Other.Offsets.at(i) < This.Offsets.at(i) + This.Sizes.at(i)) {
      ++OverlappingDims;
    }
  }
  return OverlappingDims == Offsets.size();
}
}
}
}
