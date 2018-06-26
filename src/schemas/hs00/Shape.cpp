#include "Shape.h"
#include "Exceptions.h"
#include "Writer.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename EdgeType>
Shape<EdgeType> Shape<EdgeType>::createFromJSON(json const &Json) {
  if (!Json.is_array()) {
    throw UnexpectedJsonInput();
  }
  throw unimplemented();
}

template Shape<double> Shape<double>::createFromJSON(json const &Json);
}
}
}
