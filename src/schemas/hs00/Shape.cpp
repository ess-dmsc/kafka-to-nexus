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
  throw unimplemented();
}

template Shape<uint32_t> Shape<uint32_t>::createFromJson(json const &Json);
template Shape<uint64_t> Shape<uint64_t>::createFromJson(json const &Json);
template Shape<double> Shape<double>::createFromJson(json const &Json);
}
}
}
