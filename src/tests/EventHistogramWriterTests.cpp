#include "json.h"
#include "schemas/hs00/Dimension.h"
#include "schemas/hs00/Exceptions.h"
#include "schemas/hs00/Shape.h"
#include <gtest/gtest.h>

using json = nlohmann::json;
using FileWriter::Schemas::hs00::UnexpectedJsonInput;
using FileWriter::Schemas::hs00::Shape;

TEST(EventHistogramWriter, ShapeFromJsonThrowsIfInvalidInput) {
  // The value of "shape" should be an array, so this should throw:
  auto Json = json::parse(R"""({ "shape": {} })""");
  ASSERT_THROW(Shape<double>::createFromJson(Json), UnexpectedJsonInput);
}
