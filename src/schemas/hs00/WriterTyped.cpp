#include "WriterTyped.h"
#include "Exceptions.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename DataType, typename EdgeType>
WriterTyped<DataType, EdgeType>
WriterTyped<DataType, EdgeType>::createFromJson(json const &Json) {
  if (!Json.is_object()) {
    throw UnexpectedJsonInput();
  }
  WriterTyped<DataType, EdgeType> TheWriterTyped;
  try {
    TheWriterTyped.SourceName = Json.at("source_name");
    TheWriterTyped.TheShape = Shape<EdgeType>::createFromJson(Json.at("shape"));
  } catch (json::out_of_range const &) {
    std::throw_with_nested(UnexpectedJsonInput());
  }
  return TheWriterTyped;
}

template WriterTyped<uint64_t, double>
WriterTyped<uint64_t, double>::createFromJson(json const &Json);
}
}
}
