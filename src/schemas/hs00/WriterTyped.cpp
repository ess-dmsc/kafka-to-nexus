// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

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
template <> Array getMatchingFlatbufferType(float *) {
  return Array::ArrayFloat;
}
} // namespace hs00
} // namespace Schemas
} // namespace FileWriter
