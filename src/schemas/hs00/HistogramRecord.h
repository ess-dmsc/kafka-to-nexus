#pragma once

#include "Slice.h"
#include "json.h"
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

class HistogramRecord {
public:
  static HistogramRecord create();

private:
  uint64_t Timestamp = 0;
  std::vector<Slice> Slices;
};
}
}
}
