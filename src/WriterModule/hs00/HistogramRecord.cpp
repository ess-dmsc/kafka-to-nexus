// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HistogramRecord.h"
#include "Exceptions.h"
#include "hs00_Writer.h"

namespace WriterModule {
namespace hs00 {

HistogramRecord HistogramRecord::create(size_t HDFIndex, size_t TotalItems) {
  HistogramRecord TheHistogramRecord;
  TheHistogramRecord.HDFIndex = HDFIndex;
  TheHistogramRecord.TotalItems = TotalItems;
  return TheHistogramRecord;
}

bool HistogramRecord::hasEmptySlice(Slice const &Slice) {
  for (auto &S : Slices) {
    // cppcheck-suppress useStlAlgorithm
    if (S.doesOverlap(Slice)) {
      return false;
    }
  }
  return true;
}

void HistogramRecord::addSlice(Slice const &Slice) {
  Slices.emplace_back(Slice);
}

size_t HistogramRecord::getHDFIndex() const { return HDFIndex; }

void HistogramRecord::addToItemsWritten(size_t Written) {
  ItemsWritten += Written;
}

bool HistogramRecord::isFull() const { return ItemsWritten == TotalItems; }
} // namespace hs00
} // namespace WriterModule
