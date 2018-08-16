#include "Msg.h"
#include "helper.h"
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
    "edges": [2, 3, 4, 5, 6],
    "dataset_name": "some_detector"
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
  auto Json = createTestDimensionJson();
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
      "edges": [2, 3, 4, 5, 6],
      "dataset_name": "x_detector"
    },
    {
      "size": 6,
      "label": "Position",
      "unit": "mm",
      "edges": [-3, -2, -1, 0, 1, 2, 3],
      "dataset_name": "y_detector"
    },
    {
      "size": 3,
      "label": "Time",
      "unit": "ns",
      "edges": [0, 2, 4, 6],
      "dataset_name": "time_binning"
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
    "data_type": "uint64",
    "error_type": "double",
    "edge_type": "double",
    "shape": [
      {
        "size": 4,
        "label": "Position",
        "unit": "mm",
        "edges": [2, 3, 4, 5, 6],
        "dataset_name": "x_detector"
      },
      {
        "size": 6,
        "label": "Position",
        "unit": "mm",
        "edges": [-3, -2, -1, 0, 1, 2, 3],
        "dataset_name": "y_detector"
      },
      {
        "size": 3,
        "label": "Time",
        "unit": "ns",
        "edges": [0, 2, 4, 6],
        "dataset_name": "time_binning"
      }
    ]
  })"");
}

TEST(EventHistogramWriter, WriterTypedWithoutShapeThrows) {
  auto Json = createTestWriterTypedJson();
  Json.erase("shape");
  ASSERT_THROW((WriterTyped<uint64_t, double, uint64_t>::createFromJson(Json)),
               UnexpectedJsonInput);
}

TEST(EventHistogramWriter, WriterTypedCreatedFromValidJsonInput) {
  auto TheWriterTyped = WriterTyped<uint64_t, double, uint64_t>::createFromJson(
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
  auto TheWriterTyped =
      WriterTyped<uint64_t, double, uint64_t>::createFromJson(Json);
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
  auto TheWriterTyped =
      WriterTyped<uint64_t, double, uint64_t>::createFromJson(Json);
  auto File = createFile("Test.EventHistogramWriter.WriterTypedReopen",
                         FileCreationLocation::Default);
  auto Group = File.root();
  size_t ChunkBytes = 64 * 1024;
  TheWriterTyped->createHDFStructure(Group, ChunkBytes);
  TheWriterTyped =
      WriterTyped<uint64_t, double, uint64_t>::createFromHDF(Group);
}

uint64_t getValueAtFlatIndex(size_t Index) {
  size_t I0 = Index / (3 * 6);
  Index = Index % (3 * 6);
  size_t I1 = Index / 6;
  Index = Index % 6;
  size_t I2 = Index;
  return 100 * (1 + I0) + 10 * (1 + I1) + (1 + I2);
}

FileWriter::Msg createTestMessage(size_t HistogramID, size_t PacketID) {
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
  std::vector<uint32_t> ThisOffsets{(uint32_t(PacketID) / 2) * 2,
                                    (uint32_t(PacketID) % 2) * 3, 0};

  uint64_t Timestamp = static_cast<uint64_t>((1 + HistogramID) * 1000);
  auto ThisLengthsVector = Builder.CreateVector(ThisLengths);
  auto ThisOffsetsVector = Builder.CreateVector(ThisOffsets);

  flatbuffers::Offset<void> DataValue;
  {
    size_t TotalElements = 1;
    for (auto x : ThisLengths) {
      TotalElements *= x;
    }
    std::vector<uint64_t> Data(TotalElements);
    size_t N = 0;
    for (size_t I0 = 0; I0 < ThisLengths.at(0); ++I0) {
      for (size_t I1 = 0; I1 < ThisLengths.at(1); ++I1) {
        for (size_t I2 = 0; I2 < ThisLengths.at(2); ++I2) {
          size_t O0 = ThisOffsets.at(0);
          size_t O1 = ThisOffsets.at(1);
          size_t O2 = ThisOffsets.at(2);
          size_t K0 = 1 + I0 + O0;
          size_t K1 = 1 + I1 + O1;
          size_t K2 = 1 + I2 + O2;
          Data.at(N) = Timestamp + 100 * K0 + 10 * K1 + K2;
          // Data.at(N) = Timestamp + getValueAtFlatIndex(6*3*(O0+I0) +
          // 3*(O1+I1) + (O2+I2));
          ++N;
        }
      }
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
      Data.at(i) = i * 1e-5;
    }
    auto Vec = Builder.CreateVector(Data);
    ArrayDoubleBuilder ArrayBuilder(Builder);
    ArrayBuilder.add_value(Vec);
    ErrorValue = ArrayBuilder.Finish().Union();
  }

  flatbuffers::Offset<flatbuffers::String> Info;
  if (PacketID == 0) {
    Info = Builder.CreateString("Some optional info string.");
  }

  EventHistogramBuilder EHBuilder(Builder);
  EHBuilder.add_timestamp(Timestamp);
  EHBuilder.add_dim_metadata(DMDA);
  EHBuilder.add_current_shape(ThisLengthsVector);
  EHBuilder.add_offset(ThisOffsetsVector);
  EHBuilder.add_data_type(Array::ArrayULong);
  EHBuilder.add_data(DataValue);
  EHBuilder.add_errors_type(Array::ArrayDouble);
  EHBuilder.add_errors(ErrorValue);
  if (!Info.IsNull()) {
    EHBuilder.add_info(Info);
  }
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
    auto X = Writer->write(createTestMessage(0, i));
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
                         FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump(), "{}");
  ASSERT_TRUE(Writer->init_hdf(Group, "{}").is_OK());
  Writer = Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump(), "{}");
  ASSERT_TRUE(Writer->reopen(Group).is_OK());
  size_t HistogramID = 0;
  for (size_t i = 0; i < 3; ++i) {
    auto X = Writer->write(createTestMessage(HistogramID, i));
    if (!X.is_OK()) {
      throw std::runtime_error(X.to_str());
    }
    ASSERT_TRUE(X.is_OK());
  }
  ++HistogramID;
  for (size_t i = 0; i < 4; ++i) {
    auto X = Writer->write(createTestMessage(HistogramID, i));
    if (!X.is_OK()) {
      throw std::runtime_error(X.to_str());
    }
    ASSERT_TRUE(X.is_OK());
  }
  ++HistogramID;
  for (size_t i = 1; i < 4; ++i) {
    auto X = Writer->write(createTestMessage(HistogramID, i));
    if (!X.is_OK()) {
      throw std::runtime_error(X.to_str());
    }
    ASSERT_TRUE(X.is_OK());
  }
  Writer->close();
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

TEST(EventHistogramWriter, WriteAMORExample) {
  auto File = createFile("Test.EventHistogramWriter.WriteAMORExample",
                         FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = Writer::create();
  auto V1 = gulp("/s/amor-hs00-stream");
  auto V2 = gulp("/s/amor-msg");
  if (V1.empty() && V2.empty()) {
    return;
  }
  std::string JsonBulk(V1.data(), V1.data() + V1.size());
  Writer->parse_config(JsonBulk, "{}");
  ASSERT_TRUE(Writer->init_hdf(Group, "{}").is_OK());
  Writer = Writer::create();
  Writer->parse_config(JsonBulk, "{}");
  ASSERT_TRUE(Writer->reopen(Group).is_OK());
  auto M = FileWriter::Msg::owned(reinterpret_cast<const char *>(V2.data()),
                                  V2.size());
  auto X = Writer->write(M);
  if (!X.is_OK()) {
    throw std::runtime_error(X.to_str());
  }
  ASSERT_TRUE(X.is_OK());
}
