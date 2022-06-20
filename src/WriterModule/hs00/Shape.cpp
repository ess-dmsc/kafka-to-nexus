// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Shape.h"
#include "Exceptions.h"
#include "hs00_Writer.h"

namespace WriterModule {
namespace hs00 {

template <typename EdgeType>
Shape<EdgeType> Shape<EdgeType>::createFromJson(json const &Json) {
  if (!Json.is_array()) {
    throw UnexpectedJsonInput(R"("shape" field is not an array.)");
  }
  Shape TheShape;
  for (auto const &D : Json) {
    TheShape.Dimensions.push_back(Dimension<EdgeType>::createFromJson(D));
  }
  return TheShape;
}

template <typename EdgeType> size_t Shape<EdgeType>::getNDIM() const {
  return Dimensions.size();
}

template <typename EdgeType>
std::vector<Dimension<EdgeType>> const &Shape<EdgeType>::getDimensions() const {
  return Dimensions;
}

template <typename EdgeType> size_t Shape<EdgeType>::getTotalItems() const {
  if (Dimensions.empty()) {
    return 0;
  }
  size_t N = 1;
  for (auto &D : Dimensions) {
    N *= D.getSize();
  }
  return N;
}

template Shape<uint32_t> Shape<uint32_t>::createFromJson(json const &Json);
template Shape<uint64_t> Shape<uint64_t>::createFromJson(json const &Json);
template Shape<double> Shape<double>::createFromJson(json const &Json);
template Shape<float> Shape<float>::createFromJson(json const &Json);

template size_t Shape<uint32_t>::getNDIM() const;
template size_t Shape<uint64_t>::getNDIM() const;
template size_t Shape<double>::getNDIM() const;
template size_t Shape<float>::getNDIM() const;

template std::vector<Dimension<uint32_t>> const &
Shape<uint32_t>::getDimensions() const;
template std::vector<Dimension<uint64_t>> const &
Shape<uint64_t>::getDimensions() const;
template std::vector<Dimension<double>> const &
Shape<double>::getDimensions() const;
template std::vector<Dimension<float>> const &
Shape<float>::getDimensions() const;

template size_t Shape<uint32_t>::getTotalItems() const;
template size_t Shape<uint64_t>::getTotalItems() const;
template size_t Shape<double>::getTotalItems() const;
template size_t Shape<float>::getTotalItems() const;
} // namespace hs00
} // namespace WriterModule
