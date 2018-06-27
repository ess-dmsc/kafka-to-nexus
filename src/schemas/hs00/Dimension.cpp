#include "Dimension.h"
#include "Exceptions.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename EdgeType>
Dimension<EdgeType> Dimension<EdgeType>::createFromJson(json const &Json) {
  if (!Json.is_array()) {
    throw UnexpectedJsonInput();
  }
  throw unimplemented();
}

template Dimension<double> Dimension<double>::createFromJson(json const &Json);
}
}
}
