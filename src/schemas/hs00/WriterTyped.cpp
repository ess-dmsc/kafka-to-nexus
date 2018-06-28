#include "WriterTyped.h"
#include "Exceptions.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename DataType, typename EdgeType>
WriterTyped<DataType, EdgeType>
WriterTyped<DataType, EdgeType>::createFromJson(json const &Json) {
  throw unimplemented();
}

template WriterTyped<uint64_t, double>
WriterTyped<uint64_t, double>::createFromJson(json const &Json);
}
}
}
