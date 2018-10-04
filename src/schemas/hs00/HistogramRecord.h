#pragma once

#include "Slice.h"
#include "json.h"
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

class HistogramRecord {
public:
  static HistogramRecord fromHDFIndex(size_t HDFIndex_);
  bool hasEmptySlice(Slice const &Slice);
  void addSlice(Slice Slice);
  size_t getHDFIndex() const;

private:
  size_t HDFIndex = !0;
  uint64_t Timestamp = 0;
  std::vector<Slice> Slices;
};
}
}
}
