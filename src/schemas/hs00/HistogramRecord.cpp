#include "HistogramRecord.h"
#include "Exceptions.h"
#include "Writer.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

HistogramRecord HistogramRecord::fromHDFIndex(size_t HDFIndex_) {
  HistogramRecord TheHistogramRecord;
  TheHistogramRecord.HDFIndex = HDFIndex_;
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
}
}
}
