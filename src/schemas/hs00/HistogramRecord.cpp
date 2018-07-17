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
}
}
}
