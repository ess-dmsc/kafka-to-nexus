#include "HistogramRecord.h"
#include "Exceptions.h"
#include "Writer.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

HistogramRecord HistogramRecord::create() {
  HistogramRecord TheHistogramRecord;
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
}
}
}
