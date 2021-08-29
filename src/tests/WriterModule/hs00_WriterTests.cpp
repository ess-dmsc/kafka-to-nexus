// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "AccessMessageMetadata/hs00/hs00_Extractor.h"
#include "WriterModule/hs00/Dimension.h"
#include "WriterModule/hs00/Exceptions.h"
#include "WriterModule/hs00/Shape.h"
#include "WriterModule/hs00/Slice.h"
#include "WriterModule/hs00/WriterTyped.h"
#include "WriterModule/hs00/hs00_Writer.h"
#include "WriterRegistrar.h"
#include "helper.h"
#include "helpers/SetExtractorModule.h"
#include "json.h"
#include <WriterModuleBase.h>
#include <flatbuffers/flatbuffers.h>
#include <fstream>
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>
#include <memory>

std::vector<char> readFileIntoVector(std::string const &FileName) {
  std::vector<char> ret;
  std::ifstream ifs(FileName, std::ios::binary | std::ios::ate);
  if (!ifs.good()) {
    return ret;
  }
  auto n1 = ifs.tellg();
  if (n1 <= 0) {
    return ret;
  }
  ret.resize(n1);
  ifs.seekg(0);
  ifs.read(ret.data(), n1);
  return ret;
}

using json = nlohmann::json;
using WriterModule::hs00::Dimension;
using WriterModule::hs00::hs00_Writer;
using WriterModule::hs00::Shape;
using WriterModule::hs00::Slice;
using WriterModule::hs00::UnexpectedJsonInput;
using WriterModule::hs00::WriterTyped;

class EventHistogramWriter : public ::testing::Test {
public:
  void SetUp() override {
    setExtractorModule<AccessMessageMetadata::hs00_Extractor>("hs00");
    try {
      WriterModule::Registry::Registrar<hs00_Writer> RegisterIt("hs00",
                                                                "test_name");
    } catch (...) {
    }
  }
};

json createTestDimensionJson() {
  return json::parse(R"""({
    "size": 4,
    "label": "Velocity",
    "unit": "m/s",
    "edges": [2, 3, 4, 5, 6],
    "dataset_name": "some_detector"
  })""");
}

TEST_F(EventHistogramWriter, DimensionWithoutSizeThrows) {
  auto Json = createTestDimensionJson();
  Json.erase("size");
  ASSERT_THROW(Dimension<double>::createFromJson(Json), UnexpectedJsonInput);
}

TEST_F(EventHistogramWriter, DimensionWithoutLabelThrows) {
  auto Json = createTestDimensionJson();
  Json.erase("label");
  ASSERT_THROW(Dimension<double>::createFromJson(Json), UnexpectedJsonInput);
}

TEST_F(EventHistogramWriter, DimensionWithoutUnitThrows) {
  auto Json = createTestDimensionJson();
  Json.erase("unit");
  ASSERT_THROW(Dimension<double>::createFromJson(Json), UnexpectedJsonInput);
}

TEST_F(EventHistogramWriter, DimensionCreatedFromValidInput) {
  auto Json = createTestDimensionJson();
  auto Dim = Dimension<double>::createFromJson(Json);
  ASSERT_EQ(Dim.getSize(), 4u);
  ASSERT_EQ(Dim.getLabel(), "Velocity");
  ASSERT_EQ(Dim.getUnit(), "m/s");
  ASSERT_EQ(Dim.getEdges().size(), 5u);
}

TEST_F(EventHistogramWriter, ShapeFromJsonThrowsIfInvalidInput) {
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

TEST_F(EventHistogramWriter, ShapeCreatedFromValidInput) {
  auto Json = createTestShapeJson();
  auto TheShape = Shape<double>::createFromJson(Json);
  ASSERT_EQ(TheShape.getNDIM(), 3u);
}

TEST_F(EventHistogramWriter, SliceNoOverlap) {
  auto A = Slice::fromOffsetsSizes({0}, {2});
  auto B = Slice::fromOffsetsSizes({2}, {2});
  ASSERT_FALSE(A.doesOverlap(B));
  ASSERT_FALSE(B.doesOverlap(A));
  A = Slice::fromOffsetsSizes({0, 6}, {2, 2});
  B = Slice::fromOffsetsSizes({2, 2}, {2, 2});
  ASSERT_FALSE(A.doesOverlap(B));
  ASSERT_FALSE(B.doesOverlap(A));
}

TEST_F(EventHistogramWriter, SliceOverfullOverlap) {
  auto A = Slice::fromOffsetsSizes({1, 6}, {2, 2});
  auto B = Slice::fromOffsetsSizes({0, 2}, {4, 8});
  ASSERT_TRUE(A.doesOverlap(B));
  ASSERT_TRUE(B.doesOverlap(A));
}

TEST_F(EventHistogramWriter, SlicePartialOverlap) {
  auto A = Slice::fromOffsetsSizes({1, 6}, {2, 2});
  auto B = Slice::fromOffsetsSizes({0, 7}, {2, 1});
  ASSERT_TRUE(A.doesOverlap(B));
  ASSERT_TRUE(B.doesOverlap(A));
}

json createTestWriterTypedJson() {
  auto Json = json::parse(R""({
    "data_type": "uint64",
    "error_type": "double",
    "edge_type": "float",
    "shape": [
      {
        "size": 1,
        "label": "Position",
        "unit": "mm",
        "edges": [],
        "dataset_name": "x_detector"
      },
      {
        "size": 1,
        "label": "Position",
        "unit": "mm",
        "edges": [],
        "dataset_name": "y_detector"
      },
      {
        "size": 1,
        "label": "Time",
        "unit": "ns",
        "edges": [],
        "dataset_name": "time_binning"
      }
    ]
  })"");
  Json["shape"][0]["size"] = 4;
  Json["shape"][1]["size"] = 2;
  Json["shape"][2]["size"] = 2;
  for (auto &S : Json["shape"]) {
    for (size_t I = 0; I <= S["size"]; ++I) {
      S["edges"].push_back(double(I));
    }
  }
  return Json;
}

TEST_F(EventHistogramWriter, WriterTypedWithoutShapeThrows) {
  auto Json = createTestWriterTypedJson();
  Json.erase("shape");
  ASSERT_THROW((WriterTyped<uint64_t, double, uint64_t>::create(Json)),
               UnexpectedJsonInput);
}

TEST_F(EventHistogramWriter, WriterTypedCreatedFromValidJsonInput) {
  WriterTyped<uint64_t, double, uint64_t>::create(createTestWriterTypedJson());
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

TEST_F(EventHistogramWriter, WriterTypedCreateHDFStructure) {
  auto Json = createTestWriterTypedJson();
  auto TheWriterTyped = WriterTyped<uint64_t, double, uint64_t>::create(Json);
  auto File =
      createFile("Test.EventHistogramWriter.WriterTypedCreateHDFStructure",
                 FileCreationLocation::Default);
  auto Group = File.root();
  size_t ChunkBytes = 2 * 1024 * 1024;
  TheWriterTyped->createHDFStructure(Group, ChunkBytes);
  std::string StoredJson;
  Group.attributes["created_from_json"].read(StoredJson);
  ASSERT_EQ(json::parse(StoredJson), Json);
  Group.get_dataset("histograms");
  ASSERT_EQ(Group.get_dataset("x_detector").datatype(),
            hdf5::datatype::create<double>().native_type());
}

TEST_F(EventHistogramWriter, WriterTypedReopen) {
  auto Json = createTestWriterTypedJson();
  auto TheWriterTyped = WriterTyped<uint64_t, double, uint64_t>::create(Json);
  auto File = createFile("Test.EventHistogramWriter.WriterTypedReopen",
                         FileCreationLocation::Default);
  auto Group = File.root();
  size_t ChunkBytes = 2 * 1024 * 1024;
  TheWriterTyped->createHDFStructure(Group, ChunkBytes);
  WriterTyped<uint64_t, double, uint64_t>::reOpen(Group);
}

uint64_t getValueAtFlatIndex(uint32_t HistogramID, size_t Index,
                             std::vector<uint32_t> const &DimLengths) {
  auto const &Sizes = DimLengths;
  uint32_t F = Sizes.at(2) * Sizes.at(1);
  uint32_t I2 = Index / F;
  Index = Index % F;
  F = Sizes.at(2);
  uint32_t I1 = Index / F;
  Index = Index % F;
  uint32_t I0 = Index;
  return (1 + I0) +
         100 * ((1 + I1) + 100 * ((1 + I2) + 100 * (HistogramID + 1)));
}

std::unique_ptr<flatbuffers::FlatBufferBuilder>
createTestMessage(size_t HistogramID, size_t PacketID,
                  std::vector<uint32_t> const &DimLengths) {
  namespace hs00 = WriterModule::hs00;
  auto BuilderPtr = std::make_unique<flatbuffers::FlatBufferBuilder>();
  auto &Builder = *BuilderPtr;
  flatbuffers::Offset<void> BinBoundaries;
  {
    auto Vec = Builder.CreateVector(std::vector<double>({1, 2, 3, 4}));
    hs00::ArrayDoubleBuilder ArrayBuilder(Builder);
    ArrayBuilder.add_value(Vec);
    BinBoundaries = ArrayBuilder.Finish().Union();
  }

  std::vector<flatbuffers::Offset<hs00::DimensionMetaData>> DMDs;
  for (auto Length : DimLengths) {
    hs00::DimensionMetaDataBuilder DMDBuilder(Builder);
    DMDBuilder.add_length(Length);
    DMDBuilder.add_bin_boundaries(BinBoundaries);
    DMDs.push_back(DMDBuilder.Finish());
  }
  auto DMDA = Builder.CreateVector(DMDs);

  std::vector<uint32_t> ThisDivisors{2, 2, 1};
  std::vector<uint32_t> ThisLengths = DimLengths;
  for (size_t I = 0; I < ThisLengths.size(); ++I) {
    ThisLengths.at(I) /= ThisDivisors.at(I);
  }
  std::vector<uint32_t> ThisOffsets{
      (uint32_t(PacketID) / 2) * ThisLengths.at(0),
      (uint32_t(PacketID) % 2) * ThisLengths.at(1), 0};

  auto Timestamp = static_cast<uint64_t>((1 + HistogramID) * 1000000);
  auto ThisLengthsVector = Builder.CreateVector(ThisLengths);
  auto ThisOffsetsVector = Builder.CreateVector(ThisOffsets);

  flatbuffers::Offset<void> DataValue;
  {
    size_t TotalElements =
        std::accumulate(ThisLengths.cbegin(), ThisLengths.cend(), size_t(1),
                        std::multiplies<>());

    std::vector<uint64_t> Data(TotalElements);
    size_t N = 0;
    for (size_t I0 = 0; I0 < ThisLengths.at(0); ++I0) {
      for (size_t I1 = 0; I1 < ThisLengths.at(1); ++I1) {
        for (size_t I2 = 0; I2 < ThisLengths.at(2); ++I2) {
          size_t O0 = ThisOffsets.at(0);
          size_t O1 = ThisOffsets.at(1);
          size_t O2 = ThisOffsets.at(2);
          Data.at(N) = 0;
          size_t Flat =
              (O2 + I2) +
              DimLengths.at(2) * ((O1 + I1) + DimLengths.at(1) * (O0 + I0));
          Data.at(N) = getValueAtFlatIndex(HistogramID, Flat, DimLengths);
          ++N;
        }
      }
    }
    auto Vec = Builder.CreateVector(Data);
    hs00::ArrayULongBuilder ArrayBuilder(Builder);
    ArrayBuilder.add_value(Vec);
    DataValue = ArrayBuilder.Finish().Union();
  }

  flatbuffers::Offset<void> ErrorValue;
  {
    size_t TotalElements =
        std::accumulate(ThisLengths.cbegin(), ThisLengths.cend(), size_t(1),
                        std::multiplies<>());

    std::vector<double> Data(TotalElements);
    for (size_t i = 0; i < Data.size(); ++i) {
      Data.at(i) = i * 1e-5;
    }
    auto Vec = Builder.CreateVector(Data);
    hs00::ArrayDoubleBuilder ArrayBuilder(Builder);
    ArrayBuilder.add_value(Vec);
    ErrorValue = ArrayBuilder.Finish().Union();
  }

  flatbuffers::Offset<flatbuffers::String> Info;
  if (PacketID == 0) {
    Info = Builder.CreateString("Some optional info string.");
  }

  hs00::EventHistogramBuilder EHBuilder(Builder);
  EHBuilder.add_timestamp(Timestamp);
  EHBuilder.add_dim_metadata(DMDA);
  EHBuilder.add_current_shape(ThisLengthsVector);
  EHBuilder.add_offset(ThisOffsetsVector);
  EHBuilder.add_data_type(hs00::Array::ArrayULong);
  EHBuilder.add_data(DataValue);
  EHBuilder.add_errors_type(hs00::Array::ArrayDouble);
  EHBuilder.add_errors(ErrorValue);
  if (!Info.IsNull()) {
    EHBuilder.add_info(Info);
  }
  FinishEventHistogramBuffer(Builder, EHBuilder.Finish());
  return BuilderPtr;
}

FileWriter::FlatbufferMessage
wrapBuilder(std::unique_ptr<flatbuffers::FlatBufferBuilder> const &Builder) {
  return FileWriter::FlatbufferMessage(Builder->GetBufferPointer(),
                                       Builder->GetSize());
}

using WriterModule::InitResult;

TEST_F(EventHistogramWriter, WriterInitHDF) {
  auto File = createFile("Test.EventHistogramWriter.WriterInitHDF",
                         FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = hs00_Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump());
  ASSERT_TRUE(Writer->init(Group) == InitResult::OK);
}

TEST_F(EventHistogramWriter, WriterReopen) {
  auto File = createFile("Test.EventHistogramWriter.WriterReopen",
                         FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = hs00_Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump());
  ASSERT_TRUE(Writer->init(Group) == InitResult::OK);
  Writer = hs00_Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump());
  ASSERT_TRUE(Writer->reopen(Group) == InitResult::OK);
}

TEST_F(EventHistogramWriter, WriteFullHistogramFromMultipleMessages) {
  auto File = createFile(
      "Test.EventHistogramWriter.WriteFullHistogramFromMultipleMessages",
      FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = hs00_Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump());
  ASSERT_TRUE(Writer->init(Group) == InitResult::OK);
  Writer = hs00_Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump());
  ASSERT_TRUE(Writer->reopen(Group) == InitResult::OK);
  std::vector<uint32_t> DimLengths{4, 2, 2};
  for (size_t i = 0; i < 4; ++i) {
    auto M = createTestMessage(0, i, DimLengths);
    ASSERT_NO_THROW(Writer->write(wrapBuilder(M)));
  }
  auto Histograms = Group.get_dataset("histograms");
  hdf5::dataspace::Simple Dataspace(Histograms.dataspace());
  ASSERT_EQ(Dataspace.current_dimensions().at(0), 1u);
  ASSERT_EQ(Dataspace.current_dimensions().at(1), 4u);
  ASSERT_EQ(Dataspace.current_dimensions().at(2), 2u);
  ASSERT_EQ(Dataspace.current_dimensions().at(3), 2u);
}

TEST_F(EventHistogramWriter, WriteMultipleHistograms) {
  auto File = createFile("Test.EventHistogramWriter.WriteMultipleHistograms",
                         FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = hs00_Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump());
  ASSERT_TRUE(Writer->init(Group) == InitResult::OK);
  Writer = hs00_Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump());
  ASSERT_TRUE(Writer->reopen(Group) == InitResult::OK);
  std::vector<uint32_t> DimLengths{4, 2, 2};
  size_t HistogramID = 0;
  for (size_t i = 0; i < 3; ++i) {
    auto M = createTestMessage(HistogramID, i, DimLengths);
    ASSERT_NO_THROW(Writer->write(wrapBuilder(M)));
  }
  ++HistogramID;
  for (size_t i = 0; i < 4; ++i) {
    auto M = createTestMessage(HistogramID, i, DimLengths);
    ASSERT_NO_THROW(Writer->write(wrapBuilder(M)));
  }
  ++HistogramID;
  for (size_t i = 1; i < 4; ++i) {
    auto M = createTestMessage(HistogramID, i, DimLengths);
    ASSERT_NO_THROW(Writer->write(wrapBuilder(M)));
  }
  auto Histograms = Group.get_dataset("histograms");
  hdf5::dataspace::Simple Dataspace(Histograms.dataspace());
  ASSERT_EQ(Dataspace.current_dimensions().at(0), 3u);
  ASSERT_EQ(Dataspace.current_dimensions().at(1), 4u);
  ASSERT_EQ(Dataspace.current_dimensions().at(2), 2u);
  ASSERT_EQ(Dataspace.current_dimensions().at(3), 2u);
}

TEST_F(EventHistogramWriter, WriteManyHistograms) {
  auto File = createFile("Test.EventHistogramWriter.WriteManyHistograms",
                         FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = hs00_Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump());
  ASSERT_TRUE(Writer->init(Group) == InitResult::OK);
  Writer = hs00_Writer::create();
  Writer->parse_config(createTestWriterTypedJson().dump());
  ASSERT_TRUE(Writer->reopen(Group) == InitResult::OK);
  std::vector<uint32_t> DimLengths{4, 2, 2};
  for (size_t HistogramID = 0; HistogramID < 18; ++HistogramID) {
    for (size_t i = 0; i < 4; ++i) {
      auto M = createTestMessage(HistogramID, i, DimLengths);
      ASSERT_NO_THROW(Writer->write(wrapBuilder(M)));
    }
  }
  auto Histograms = Group.get_dataset("histograms");
  hdf5::dataspace::Simple Dataspace(Histograms.dataspace());
  ASSERT_EQ(Dataspace.current_dimensions().at(0), 4u);
  ASSERT_EQ(Dataspace.current_dimensions().at(1), 4u);
  ASSERT_EQ(Dataspace.current_dimensions().at(2), 2u);
  ASSERT_EQ(Dataspace.current_dimensions().at(3), 2u);

  std::vector<uint64_t> Buffer(DimLengths.at(0) * DimLengths.at(1) *
                               DimLengths.at(2));
  Dataspace.selection(hdf5::dataspace::SelectionOperation::SET,
                      hdf5::dataspace::Hyperslab({1, 0, 0, 0}, {1, 4, 2, 2}));
  hdf5::dataspace::Simple SpaceMem({4, 2, 2});
  Histograms.read(Buffer, Histograms.datatype(), SpaceMem, Dataspace);
  for (size_t Flat = 0; Flat < Buffer.size(); ++Flat) {
    ASSERT_EQ(Buffer.at(Flat), getValueAtFlatIndex(17, Flat, DimLengths));
  }
}

TEST_F(EventHistogramWriter, WriteAMORExample) {
  auto File = createFile("Test.EventHistogramWriter.WriteAMORExample",
                         FileCreationLocation::Default);
  auto Group = File.root();
  auto Writer = hs00_Writer::create();
  auto V1 = readFileIntoVector("/s/amor-hs00-stream");
  auto V2 = readFileIntoVector("/s/amor-msg");
  if (V1.empty() && V2.empty()) {
    return;
  }
  std::string JsonBulk(V1.data(), V1.data() + V1.size());
  Writer->parse_config(JsonBulk);
  ASSERT_TRUE(Writer->init(Group) == InitResult::OK);
  Writer = hs00_Writer::create();
  Writer->parse_config(JsonBulk);
  ASSERT_TRUE(Writer->reopen(Group) == InitResult::OK);
  auto M = FileWriter::FlatbufferMessage(
      reinterpret_cast<uint8_t const *>(V2.data()), V2.size());
  ASSERT_NO_THROW(Writer->write(M));
}
