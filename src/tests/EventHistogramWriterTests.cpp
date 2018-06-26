#include "json.h"
#include "schemas/hs00/Exceptions.h"
#include "schemas/hs00/Shape.h"
#include <gtest/gtest.h>

using json = nlohmann::json;
using FileWriter::Schemas::hs00::UnexpectedJsonInput;
using FileWriter::Schemas::hs00::Shape;

TEST(EventHistogramWriter, ShapeFromJSON) {
  auto Json = json::parse(R"""({ "shape": {} })""");
  ASSERT_THROW(Shape<double>::createFromJSON(Json), UnexpectedJsonInput);
}
