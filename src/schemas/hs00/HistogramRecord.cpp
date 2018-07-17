#include "HistogramRecord.h"
#include "Exceptions.h"
#include "Writer.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

HistogramRecord HistogramRecord::create(size_t HDFIndex_, size_t TotalItems_) {
  HistogramRecord TheHistogramRecord;
  TheHistogramRecord.HDFIndex = HDFIndex_;
  TheHistogramRecord.TotalItems = TotalItems_;
  return TheHistogramRecord;
}

bool HistogramRecord::hasEmptySlice(Slice const &Slice) {
  for (auto &S : Slices) {
    if (S.doesOverlap(Slice)) {
      return false;
    }
  }
  return true;
}

void HistogramRecord::addSlice(Slice Slice) { Slices.push_back(Slice); }

size_t HistogramRecord::getHDFIndex() const { return HDFIndex; }

void HistogramRecord::addToItemsWritten(size_t Written) {
  ItemsWritten += Written;
}

bool HistogramRecord::isFull() const { return ItemsWritten == TotalItems; }
}
}
}
