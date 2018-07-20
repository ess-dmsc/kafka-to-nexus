#include "json.h"
#include "schemas/hs00/Dimension.h"
#include "schemas/hs00/Exceptions.h"
#include "schemas/hs00/Shape.h"
#include "schemas/hs00/WriterTyped.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>

using json = nlohmann::json;
using FileWriter::Schemas::hs00::UnexpectedJsonInput;
using FileWriter::Schemas::hs00::Dimension;
using FileWriter::Schemas::hs00::Shape;
using FileWriter::Schemas::hs00::WriterTyped;

json createTestDimensionJson() {
  return json::parse(R"""({
    "size": 4,
    "label": "Velocity",
    "unit": "m/s",
    "edges": [2, 3, 4, 5]
  })""");
}

TEST(EventHistogramWriter, DimensionWithoutSizeThrows) {
  auto Json = createTestDimensionJson();
  Json.erase("size");
  ASSERT_THROW(Dimension<double>::createFromJson(Json), UnexpectedJsonInput);
}

TEST(EventHistogramWriter, DimensionWithoutLabelThrows) {
  auto Json = createTestDimensionJson();
  Json.erase("label");
  ASSERT_THROW(Dimension<double>::createFromJson(Json), UnexpectedJsonInput);
}

TEST(EventHistogramWriter, DimensionWithoutUnitThrows) {
  auto Json = createTestDimensionJson();
  Json.erase("unit");
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

json createTestShapeJson() {
  return json::parse(R""([
    {
      "size": 4,
      "label": "Position",
      "unit": "mm",
      "edges": [2, 3, 4, 5, 6]
    },
    {
      "size": 6,
      "label": "Position",
      "unit": "mm",
      "edges": [-3, -2, -1, 0, 1, 2, 3]
    },
    {
      "size": 3,
      "label": "Time",
      "unit": "ns",
      "edges": [0, 2, 4, 6]
    }
  ])"");
}

TEST(EventHistogramWriter, ShapeCreatedFromValidInput) {
  auto Json = createTestShapeJson();
  auto TheShape = Shape<double>::createFromJson(Json);
  ASSERT_EQ(TheShape.getNDIM(), 3u);
}

json createTestWriterTypedJson() {
  return json::parse(R""({
    "source_name": "SomeHistogrammer",
    "data_type": "uint64_t",
    "edge_type": "double",
    "shape": [
      {
        "size": 4,
        "label": "Position",
        "unit": "mm",
        "edges": [2, 3, 4, 5, 6]
      },
      {
        "size": 6,
        "label": "Position",
        "unit": "mm",
        "edges": [-3, -2, -1, 0, 1, 2, 3]
      },
      {
        "size": 3,
        "label": "Time",
        "unit": "ns",
        "edges": [0, 2, 4, 6]
      }
    ]
  })"");
}

TEST(EventHistogramWriter, WriterTypedWithoutSourceNameThrows) {
  auto Json = createTestWriterTypedJson();
  Json.erase("source_name");
  ASSERT_THROW((WriterTyped<uint64_t, double>::createFromJson(Json)),
               UnexpectedJsonInput);
}

TEST(EventHistogramWriter, WriterTypedWithoutShapeThrows) {
  auto Json = createTestWriterTypedJson();
  Json.erase("shape");
  ASSERT_THROW((WriterTyped<uint64_t, double>::createFromJson(Json)),
               UnexpectedJsonInput);
}

TEST(EventHistogramWriter, WriterTypedCreatedFromValidJsonInput) {
  auto TheWriterTyped = WriterTyped<uint64_t, double>::createFromJson(
      createTestWriterTypedJson());
}

hdf5::file::File createFileInMemory(std::string Name) {
  hdf5::property::FileAccessList FAPL;
  FAPL.driver(hdf5::file::MemoryDriver());
  return hdf5::file::create(Name, hdf5::file::AccessFlags::TRUNCATE,
                            hdf5::property::FileCreationList(), FAPL);
}

TEST(EventHistogramWriter, WriterTypedCreateHDFStructure) {
  auto Json = createTestWriterTypedJson();
  auto TheWriterTyped = WriterTyped<uint64_t, double>::createFromJson(Json);
  auto File = createFileInMemory(
      "Test.EventHistogramWriter.WriterTypedCreateHDFStructure");
  auto Group = File.root();
  size_t ChunkBytes = 64 * 1024;
  TheWriterTyped.createHDFStructure(Group, ChunkBytes);
  std::string StoredJson;
  Group.attributes["created_from_json"].read(StoredJson);
  ASSERT_EQ(json::parse(StoredJson), Json);
  auto Dataset = Group.get_dataset("histograms");
  // Everything fine as long as we don't throw.
}

TEST(EventHistogramWriter, WriterTypedReopen) {
  auto Json = createTestWriterTypedJson();
  auto TheWriterTyped = WriterTyped<uint64_t, double>::createFromJson(Json);
  auto File = createFileInMemory("Test.EventHistogramWriter.WriterTypedReopen");
  auto Group = File.root();
  size_t ChunkBytes = 64 * 1024;
  TheWriterTyped.createHDFStructure(Group, ChunkBytes);
  TheWriterTyped = WriterTyped<uint64_t, double>::createFromHDF(Group);
}
