#pragma once

#include "Slice.h"
#include "json.h"
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

class HistogramRecord {
public:
  static HistogramRecord create(size_t HDFIndex_, size_t TotalItems_);
  bool hasEmptySlice(Slice const &Slice);
  void addSlice(Slice const &Slice);
  size_t getHDFIndex() const;
  void addToItemsWritten(size_t Written);
  bool isFull() const;

private:
  size_t HDFIndex = !0;
  std::vector<Slice> Slices;
  size_t ItemsWritten = 0;
  size_t TotalItems = 0;
};
} // namespace hs00
} // namespace Schemas
} // namespace FileWriter
