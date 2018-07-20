#include "json.h"
#include "schemas/hs00/Dimension.h"
#include "schemas/hs00/Exceptions.h"
#include "schemas/hs00/Shape.h"
#include <gtest/gtest.h>

using json = nlohmann::json;
using FileWriter::Schemas::hs00::UnexpectedJsonInput;
using FileWriter::Schemas::hs00::Dimension;
using FileWriter::Schemas::hs00::Shape;

TEST(EventHistogramWriter, DimensionWithInconsistentSizeThrows) {
  auto Json = json::parse(R"""({
		"size": 4,
		"label": "Velocity",
		"unit": "m/s",
		"edges": [2, 3, 4, 5]
	})""");
  ASSERT_THROW(Dimension<double>::createFromJson(Json), UnexpectedJsonInput);
}

TEST(EventHistogramWriter, DimensionCreatedFromValidInput) {
  auto Json = json::parse(R"""({
		"size": 4,
		"label": "Velocity",
		"unit": "m/s",
		"edges": [2, 3, 4, 5, 6]
	})""");
  auto Dim = Dimension<double>::createFromJson(Json);
  ASSERT_EQ(Dim.getSize(), 4u);
  ASSERT_EQ(Dim.getLabel(), "Velocity");
  ASSERT_EQ(Dim.getUnit(), "m/s");
  ASSERT_EQ(Dim.getEdges().size(), 5u);
}

TEST(EventHistogramWriter, ShapeFromJsonThrowsIfInvalidInput) {
  // The value of "shape" should be an array, so this should throw:
  auto Json = json::parse(R"""({ "shape": {} })""");
  ASSERT_THROW(Shape<double>::createFromJson(Json), UnexpectedJsonInput);
}
