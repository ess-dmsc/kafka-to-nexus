#include "Msg.h"
#include "json.h"
#include "schemas/hs00/Dimension.h"
#include "schemas/hs00/Exceptions.h"
#include "schemas/hs00/Shape.h"
#include "schemas/hs00/Slice.h"
#include "schemas/hs00/Writer.h"
#include "schemas/hs00/WriterTyped.h"
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>
#include <memory>

namespace FileWriter {
namespace Schemas {
namespace hs00 {
#include "schemas/hs00_event_histogram_generated.h"
}
}
}

using json = nlohmann::json;
using FileWriter::Schemas::hs00::UnexpectedJsonInput;
using FileWriter::Schemas::hs00::Dimension;
using FileWriter::Schemas::hs00::Shape;
using FileWriter::Schemas::hs00::Slice;
using FileWriter::Schemas::hs00::WriterTyped;
using FileWriter::Schemas::hs00::Writer;

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

TEST(EventHistogramWriter, SliceNoOverlap) {
  auto A = Slice::fromOffsetsSizes({0}, {2});
  auto B = Slice::fromOffsetsSizes({2}, {2});
  ASSERT_FALSE(A.doesOverlap(B));
  ASSERT_FALSE(B.doesOverlap(A));
  A = Slice::fromOffsetsSizes({0, 6}, {2, 2});
  B = Slice::fromOffsetsSizes({2, 2}, {2, 2});
  ASSERT_FALSE(A.doesOverlap(B));
  ASSERT_FALSE(B.doesOverlap(A));
}

TEST(EventHistogramWriter, SliceOverfullOverlap) {
  auto A = Slice::fromOffsetsSizes({1, 6}, {2, 2});
  auto B = Slice::fromOffsetsSizes({0, 2}, {4, 8});
  ASSERT_TRUE(A.doesOverlap(B));
  ASSERT_TRUE(B.doesOverlap(A));
}

TEST(EventHistogramWriter, SlicePartialOverlap) {
  auto A = Slice::fromOffsetsSizes({1, 6}, {2, 2});
  auto B = Slice::fromOffsetsSizes({0, 7}, {2, 1});
  ASSERT_TRUE(A.doesOverlap(B));
  ASSERT_TRUE(B.doesOverlap(A));
}

json createTestWriterTypedJson() {
  return json::parse(R""({
    "source_name": "SomeHistogrammer",
    "data_type": "uint64",
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

enum class FileCreationLocation { Default, Memory, Disk };

hdf5::file::File createFile(std::string Name, FileCreationLocation Location) {
  hdf5::property::FileAccessList FAPL;
  if (Location == FileCreationLocation::Default) {
    Location = FileCreationLocation::Memory;
  }
  if (Location == FileCreationLocation::Memory) {
    FAPL.driver(hdf5::file::MemoryDriver());
  }
  return hdf5::file::create(Name, hdf5::file::AccessFlags::TRUNCATE,
                            hdf5::property::FileCreationList(), FAPL);
}

TEST(EventHistogramWriter, WriterTypedCreateHDFStructure) {
  auto Json = createTestWriterTypedJson();
  auto TheWriterTyped = WriterTyped<uint64_t, double>::createFromJson(Json);
  auto File =
      createFile("Test.EventHistogramWriter.WriterTypedCreateHDFStructure",
                 FileCreationLocation::Default);
  auto Group = File.root();
  size_t ChunkBytes = 64 * 1024;
  TheWriterTyped->createHDFStructure(Group, ChunkBytes);
  std::string StoredJson;
  Group.attributes["created_from_json"].read(StoredJson);
  ASSERT_EQ(json::parse(StoredJson), Json);
  auto Dataset = Group.get_dataset("histograms");
  // Everything fine as long as we don't throw.
}

TEST(EventHistogramWriter, WriterTypedReopen) {
  auto Json = createTestWriterTypedJson();
  auto TheWriterTyped = WriterTyped<uint64_t, double>::createFromJson(Json);
  auto File = createFile("Test.EventHistogramWriter.WriterTypedReopen",
                         FileCreationLocation::Default);
  auto Group = File.root();
  size_t ChunkBytes = 64 * 1024;
  TheWriterTyped->createHDFStructure(Group, ChunkBytes);
  TheWriterTyped = WriterTyped<uint64_t, double>::createFromHDF(Group);
}

FileWriter::Msg createTestMessage(uint64_t Timestamp, size_t ix, uint64_t V0) {
  using namespace FileWriter::Schemas::hs00;
  auto BuilderPtr = std::unique_ptr<flatbuffers::FlatBufferBuilder>(
      new flatbuffers::FlatBufferBuilder);
  auto &Builder = *BuilderPtr;
  flatbuffers::Offset<void> BinBoundaries;
  {
    auto Vec = Builder.CreateVector(std::vector<double>({1, 2, 3, 4}));
    ArrayDoubleBuilder ArrayBuilder(Builder);
    ArrayBuilder.add_value(Vec);
    BinBoundaries = ArrayBuilder.Finish().Union();
  }

  std::vector<size_t> DimLengths{4, 6, 3};
  std::vector<flatbuffers::Offset<DimensionMetaData>> DMDs;
  for (auto Length : DimLengths) {
    DimensionMetaDataBuilder DMDBuilder(Builder);
    DMDBuilder.add_length(Length);
    DMDBuilder.add_bin_boundaries(BinBoundaries);
    DMDs.push_back(DMDBuilder.Finish());
  }
  auto DMDA = Builder.CreateVector(DMDs);

  std::vector<uint32_t> ThisLengths{2, 3, 3};
  std::vector<uint32_t> ThisOffsets{(uint32_t(ix) / 2) * 2,
                                    (uint32_t(ix) % 2) * 3, 0};

  auto ThisLengthsVector = Builder.CreateVector(ThisLengths);
  auto ThisOffsetsVector = Builder.CreateVector(ThisOffsets);

  flatbuffers::Offset<void> DataValue;
  {
    size_t TotalElements = 1;
    for (auto x : ThisLengths) {
      TotalElements *= x;
    }
    std::vector<uint64_t> Data(TotalElements);
    for (size_t i = 0; i < Data.size(); ++i) {
      Data.at(i) = V0 + i;
    }
    auto Vec = Builder.CreateVector(Data);
    ArrayULongBuilder ArrayBuilder(Builder);
    ArrayBuilder.add_value(Vec);
    DataValue = ArrayBuilder.Finish().Union();
  }

  flatbuffers::Offset<void> ErrorValue;
  {
    size_t TotalElements = 1;
    for (auto x : ThisLengths) {
      TotalElements *= x;
    }
    std::vector<double> Data(TotalElements);
    for (size_t i = 0; i < Data.size(); ++i) {
      Data.at(i) = (V0 + i) / 10;
    }
    auto Vec = Builder.CreateVector(Data);
    ArrayDoubleBuilder ArrayBuilder(Builder);
    ArrayBuilder.add_value(Vec);
    ErrorValue = ArrayBuilder.Finish().Union();
  }

  auto Source = Builder.CreateString("Testsource");

  EventHistogramBuilder EHBuilder(Builder);
  EHBuilder.add_source(Source);
  EHBuilder.add_timestamp(Timestamp);
  EHBuilder.add_dim_metadata(DMDA);
  EHBuilder.add_current_shape(ThisLengthsVector);
  EHBuilder.add_offset(ThisOffsetsVector);
  EHBuilder.add_data_type(Array::ArrayULong);
  EHBuilder.add_data(DataValue);
  EHBuilder.add_errors_type(Array::ArrayDouble);
  EHBuilder.add_errors(ErrorValue);
  FinishEventHistogramBuffer(Builder, EHBuilder.Finish());
  return FileWriter::Msg::owned(
      reinterpret_cast<const char *>(Builder.GetBufferPointer()),
      Builder.GetSize());
}

TEST(EventHistogramWriter, WriterInitHDF) {
  auto File = createFile("Test.EventHistogramWriter.WriterInitHDF",
                         FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump(), "{}");
  ASSERT_TRUE(Writer->init_hdf(Group, "{}").is_OK());
}

TEST(EventHistogramWriter, WriterReopen) {
  auto File = createFile("Test.EventHistogramWriter.WriterReopen",
                         FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump(), "{}");
  ASSERT_TRUE(Writer->init_hdf(Group, "{}").is_OK());
  Writer = Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump(), "{}");
  ASSERT_TRUE(Writer->reopen(Group).is_OK());
}

TEST(EventHistogramWriter, WriteFullHistogramFromMultipleMessages) {
  auto File = createFile(
      "Test.EventHistogramWriter.WriteFullHistogramFromMultipleMessages",
      FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump(), "{}");
  ASSERT_TRUE(Writer->init_hdf(Group, "{}").is_OK());
  Writer = Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump(), "{}");
  ASSERT_TRUE(Writer->reopen(Group).is_OK());
  for (size_t i = 0; i < 4; ++i) {
    auto X = Writer->write(createTestMessage(300, i, 100 * (1 + i)));
    if (!X.is_OK()) {
      throw std::runtime_error(X.to_str());
    }
    ASSERT_TRUE(X.is_OK());
  }
  auto Histograms = Group.get_dataset("histograms");
  hdf5::dataspace::Simple Dataspace(Histograms.dataspace());
  ASSERT_EQ(Dataspace.current_dimensions().at(0), 1u);
  ASSERT_EQ(Dataspace.current_dimensions().at(1), 4u);
  ASSERT_EQ(Dataspace.current_dimensions().at(2), 6u);
  ASSERT_EQ(Dataspace.current_dimensions().at(3), 3u);
}

TEST(EventHistogramWriter, WriteMultipleHistograms) {
  auto File = createFile("Test.EventHistogramWriter.WriteMultipleHistograms",
                         FileCreationLocation::Disk);
  auto Group = File.root();
  auto Writer = Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump(), "{}");
  ASSERT_TRUE(Writer->init_hdf(Group, "{}").is_OK());
  Writer = Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump(), "{}");
  ASSERT_TRUE(Writer->reopen(Group).is_OK());
  for (size_t i = 0; i < 3; ++i) {
    auto X =
        Writer->write(createTestMessage(100000, i, 100000 + 1000 * (1 + i)));
    if (!X.is_OK()) {
      throw std::runtime_error(X.to_str());
    }
    ASSERT_TRUE(X.is_OK());
  }
  for (size_t i = 0; i < 4; ++i) {
    auto X =
        Writer->write(createTestMessage(200000, i, 200000 + 1000 * (1 + i)));
    if (!X.is_OK()) {
      throw std::runtime_error(X.to_str());
    }
    ASSERT_TRUE(X.is_OK());
  }
  for (size_t i = 1; i < 4; ++i) {
    auto X =
        Writer->write(createTestMessage(300000, i, 300000 + 1000 * (1 + i)));
    if (!X.is_OK()) {
      throw std::runtime_error(X.to_str());
    }
    ASSERT_TRUE(X.is_OK());
  }
  auto Histograms = Group.get_dataset("histograms");
  hdf5::dataspace::Simple Dataspace(Histograms.dataspace());
  ASSERT_EQ(Dataspace.current_dimensions().at(0), 3u);
  ASSERT_EQ(Dataspace.current_dimensions().at(1), 4u);
  ASSERT_EQ(Dataspace.current_dimensions().at(2), 6u);
  ASSERT_EQ(Dataspace.current_dimensions().at(3), 3u);
}

TEST(EventHistogramWriter, WriteMultipleHistogramsWithMinimumInterval) {
  // exact strategy still to be decided upon.
}
