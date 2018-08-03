#include "Shape.h"
#include "Exceptions.h"
#include "Writer.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename EdgeType>
Shape<EdgeType> Shape<EdgeType>::createFromJson(json const &Json) {
  if (!Json.is_array()) {
    throw UnexpectedJsonInput();
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

template Shape<uint32_t> Shape<uint32_t>::createFromJson(json const &Json);
template Shape<uint64_t> Shape<uint64_t>::createFromJson(json const &Json);
template Shape<double> Shape<double>::createFromJson(json const &Json);

template size_t Shape<uint32_t>::getNDIM() const;
template size_t Shape<uint64_t>::getNDIM() const;
template size_t Shape<double>::getNDIM() const;

template std::vector<Dimension<uint32_t>> const &
Shape<uint32_t>::getDimensions() const;
template std::vector<Dimension<uint64_t>> const &
Shape<uint64_t>::getDimensions() const;
template std::vector<Dimension<double>> const &
Shape<double>::getDimensions() const;
}
}
}
