#include "WriterTyped.h"
#include "../../helper.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {
template <> Array getMatchingFlatbufferType(uint32_t *) {
  return Array::ArrayUInt;
}
template <> Array getMatchingFlatbufferType(uint64_t *) {
  return Array::ArrayULong;
}
template <> Array getMatchingFlatbufferType(double *) {
  return Array::ArrayDouble;
}
}
}
}
