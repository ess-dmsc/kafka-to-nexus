// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>
#include <memory>

#include "AccessMessageMetadata/f144/f144_Extractor.h"
#include "FlatbufferMessage.h"
#include "WriterModule/f144/f144_Writer.h"
#include "helper.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"
#include <f144_logdata_generated.h>

using nlohmann::json;

using namespace WriterModule::f144_logdata;

class f144Init : public ::testing::Test {
public:
  void SetUp() override {
    TestFile =
        HDFFileTestHelper::createInMemoryTestFile("SomeTestFile.hdf5", false);
    RootGroup = TestFile->hdfGroup();
  }
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> TestFile;
  hdf5::node::Group RootGroup;
};

class f144_WriterStandIn : public f144_Writer {
public:
  using f144_Writer::ArraySize;
  using f144_Writer::ChunkSize;
  using f144_Writer::CueIndex;
  using f144_Writer::CueTimestampZero;
  using f144_Writer::ElementType;
  using f144_Writer::Timestamp;
  using f144_Writer::ValueIndexInterval;
  using f144_Writer::Values;
};

TEST_F(f144Init, BasicDefaultInit) {
  f144_Writer TestWriter;
  TestWriter.init_hdf(RootGroup);
  EXPECT_TRUE(RootGroup.has_dataset("cue_index"));
  EXPECT_TRUE(RootGroup.has_dataset("cue_timestamp_zero"));
  EXPECT_TRUE(RootGroup.has_dataset("time"));
  EXPECT_TRUE(RootGroup.has_dataset("value"));
  EXPECT_TRUE(RootGroup.has_dataset("alarm_time"));
  EXPECT_TRUE(RootGroup.has_dataset("alarm_severity"));
  EXPECT_TRUE(RootGroup.has_dataset("alarm_message"));
}

TEST_F(f144Init, ReOpenSuccess) {
  f144_Writer TestWriter;
  TestWriter.init_hdf(RootGroup);
  EXPECT_EQ(TestWriter.reopen(RootGroup), WriterModule::InitResult::OK);
}

TEST_F(f144Init, ReOpenFailure) {
  f144_Writer TestWriter;
  EXPECT_EQ(TestWriter.reopen(RootGroup), WriterModule::InitResult::ERROR);
}

TEST_F(f144Init, CheckInitDataType) {
  f144_WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup);
  auto Open = NeXusDataset::Mode::Open;
  NeXusDataset::MultiDimDatasetBase Value(RootGroup, Open);
  EXPECT_EQ(Value.datatype(), hdf5::datatype::create<double>());
}

TEST_F(f144Init, CheckValueInitShape1) {
  f144_WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup);
  auto Open = NeXusDataset::Mode::Open;
  NeXusDataset::MultiDimDatasetBase Value(RootGroup, Open);
  EXPECT_EQ(hdf5::Dimensions({0, 1}), Value.get_extent());
}

TEST_F(f144Init, CheckValueInitShape2) {
  f144_WriterStandIn TestWriter;
  TestWriter.ArraySize.setValue("", "10");
  TestWriter.init_hdf(RootGroup);
  auto Open = NeXusDataset::Mode::Open;
  NeXusDataset::MultiDimDatasetBase Value(RootGroup, Open);
  EXPECT_EQ(hdf5::Dimensions({0, 10}), Value.get_extent());
}

TEST_F(f144Init, CheckAllDataTypes) {
  std::vector<std::pair<f144_Writer::Type, hdf5::datatype::Datatype>> TypeMap{
      {f144_Writer::Type::int8, hdf5::datatype::create<std::int8_t>()},
      {f144_Writer::Type::uint8, hdf5::datatype::create<std::uint8_t>()},
      {f144_Writer::Type::int16, hdf5::datatype::create<std::int16_t>()},
      {f144_Writer::Type::uint16, hdf5::datatype::create<std::uint16_t>()},
      {f144_Writer::Type::int32, hdf5::datatype::create<std::int32_t>()},
      {f144_Writer::Type::uint32, hdf5::datatype::create<std::uint32_t>()},
      {f144_Writer::Type::int64, hdf5::datatype::create<std::int64_t>()},
      {f144_Writer::Type::uint64, hdf5::datatype::create<std::uint64_t>()},
      {f144_Writer::Type::float32, hdf5::datatype::create<float>()},
      {f144_Writer::Type::float64, hdf5::datatype::create<double>()}};
  auto Open = NeXusDataset::Mode::Open;
  f144_WriterStandIn TestWriter;
  int Ctr{0};
  for (auto &Type : TypeMap) {
    auto CurrentGroup = RootGroup.create_group("Group" + std::to_string(Ctr++));
    TestWriter.ElementType = Type.first;
    TestWriter.init_hdf(CurrentGroup);
    NeXusDataset::MultiDimDatasetBase Value(CurrentGroup, Open);
    EXPECT_EQ(Type.second, Value.datatype());
  }
}

class f144ConfigParse : public ::testing::Test {
public:
};

TEST_F(f144ConfigParse, EmptyConfig) {
  f144_WriterStandIn TestWriter;
  TestWriter.parse_config("{}");
  f144_WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ElementType, TestWriter2.ElementType);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f144ConfigParse, SetArraySize) {
  f144_WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "array_size": 3
            })");
  f144_WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, 3u);
  EXPECT_EQ(TestWriter.ElementType, TestWriter2.ElementType);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f144ConfigParse, SetChunkSize) {
  f144_WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "chunk_size": 511
            })");
  f144_WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ElementType, TestWriter2.ElementType);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ChunkSize, 511u);
}

TEST_F(f144ConfigParse, CuInterval) {
  f144_WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "cue_interval": 24
            })");
  f144_WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ElementType, TestWriter2.ElementType);
  EXPECT_EQ(TestWriter.ValueIndexInterval, 24u);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f144ConfigParse, DataType1) {
  f144_WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "type": "int8"
            })");
  f144_WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ElementType, f144_Writer::Type::int8);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f144ConfigParse, DataType2) {
  f144_WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "dtype": "uint64"
            })");
  f144_WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ElementType, f144_Writer::Type::uint64);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f144ConfigParse, DataTypeFailure) {
  f144_WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "Dtype": "uint64"
            })");
  f144_WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ElementType, f144_Writer::Type::float64);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f144ConfigParse, DataTypes) {
  using Type = f144_Writer::Type;
  std::vector<std::pair<std::string, Type>> TypeList{
      {"int8", Type::int8},       {"INT8", Type::int8},
      {"SHORT", Type::int16},     {"UINT8", Type::uint8},
      {"INT16", Type::int16},     {"Uint16", Type::uint16},
      {"int32", Type::int32},     {"Int", Type::int32},
      {"uint32", Type::uint32},   {"int64", Type::int64},
      {"long", Type::int64},      {"uint64", Type::uint64},
      {"float32", Type::float32}, {"float", Type::float32},
      {"FLOAT", Type::float32},   {"float64", Type::float64},
      {"double", Type::float64},  {"DOUBLE", Type::float64}};
  for (auto &CType : TypeList) {
    f144_WriterStandIn TestWriter;
    EXPECT_EQ(TestWriter.ElementType, Type::float64);
    TestWriter.parse_config(R"({"type":")" + CType.first + R"("})");
    EXPECT_EQ(TestWriter.ElementType, CType.second)
        << "Failed on type string: " << CType.first;
  }
}

class f144WriteData : public ::testing::Test {
public:
  void SetUp() override {
    TestFile =
        HDFFileTestHelper::createInMemoryTestFile("SomeTestFile.hdf5", false);
    RootGroup = TestFile->hdfGroup();
    setExtractorModule<AccessMessageMetadata::f144_Extractor>("f144");
  }
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> TestFile;
  hdf5::node::Group RootGroup;
};

namespace f144_schema {
  template <class ValFuncType>
  std::pair<std::unique_ptr<uint8_t[]>, size_t>
  generateFlatbufferMessageBase(ValFuncType ValueFunc, Value ValueTypeId,
                                std::uint64_t Timestamp) {
    auto Builder = flatbuffers::FlatBufferBuilder();
    auto SourceNameOffset = Builder.CreateString("SomeSourceName");
    auto ValueOffset = ValueFunc(Builder);
    LogDataBuilder LogDataBuilder(Builder);
    LogDataBuilder.add_value(ValueOffset);
    LogDataBuilder.add_timestamp(Timestamp);
    LogDataBuilder.add_source_name(SourceNameOffset);
    LogDataBuilder.add_value_type(ValueTypeId);
    
    FinishLogDataBuffer(Builder, LogDataBuilder.Finish());
    size_t BufferSize = Builder.GetSize();
    auto ReturnBuffer = std::make_unique<uint8_t[]>(BufferSize);
    std::memcpy(ReturnBuffer.get(), Builder.GetBufferPointer(), BufferSize);
    return {std::move(ReturnBuffer), BufferSize};
  }

  std::pair<std::unique_ptr<uint8_t[]>, size_t>
  generateFlatbufferMessage(double Value, std::uint64_t Timestamp) {
    auto ValueFunc = [Value](auto &Builder) {
      DoubleBuilder ValueBuilder(Builder);
      ValueBuilder.add_value(Value);
      return ValueBuilder.Finish().Union();
    };
    return generateFlatbufferMessageBase(ValueFunc, Value::Double, Timestamp);
  }
}

TEST_F(f144WriteData, ConfigUnitsAttributeOnValueDataset) {
  f144_WriterStandIn TestWriter;
  const std::string units_string = "parsecs";
  // GIVEN value_units is specified in the JSON config
  TestWriter.parse_config(
      fmt::format(R"({{"value_units": "{}"}})", units_string));

  // WHEN the writer module creates the datasets
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);

  // THEN a units attributes is created on the value dataset with the specified
  // string
  std::string attribute_value;
  EXPECT_NO_THROW(TestWriter.Values.attributes["units"].read(attribute_value))
      << "Expect units attribute to be present on the value dataset";
  EXPECT_EQ(attribute_value, units_string) << "Expect units attribute to have "
                                              "the value specified in the JSON "
                                              "configuration";
}

TEST_F(f144WriteData, ConfigUnitsAttributeOnValueDatasetIfEmpty) {
  f144_WriterStandIn TestWriter;
  // GIVEN value_units is specified as an empty string in the JSON config
  TestWriter.parse_config(R"({"value_units": ""})");

  // WHEN the writer module creates the datasets
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);

  EXPECT_FALSE(TestWriter.Values.attributes.exists("units"))
      << "units attribute should not be created if the config string is empty";
}

TEST_F(f144WriteData, UnitsAttributeOnValueDatasetNotCreatedIfNotInConfig) {
  f144_WriterStandIn TestWriter;
  // GIVEN value_units is not specified in the JSON config
  TestWriter.parse_config("{}");

  // WHEN the writer module creates the datasets
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);

  // THEN a units attributes is not created on the value dataset
  EXPECT_FALSE(TestWriter.Values.attributes.exists("units"))
      << "units attribute should not be created if it was not specified in the "
         "JSON config";
}

TEST_F(f144WriteData, WriteOneElement) {
  f144_WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);
  double ElementValue{3.14};
  std::uint64_t Timestamp{11};
  auto FlatbufferData = f144_schema::generateFlatbufferMessage(ElementValue, Timestamp);
  EXPECT_EQ(TestWriter.Values.get_extent(), hdf5::Dimensions({0, 1}));
  EXPECT_EQ(TestWriter.Timestamp.dataspace().size(), 0);
  TestWriter.write(FileWriter::FlatbufferMessage(FlatbufferData.first.get(),
                                                 FlatbufferData.second));
  ASSERT_EQ(TestWriter.Values.get_extent(), hdf5::Dimensions({1, 1}));
  ASSERT_EQ(TestWriter.Timestamp.dataspace().size(), 1);
  std::vector<double> WrittenValues(1);
  TestWriter.Values.read(WrittenValues);
  EXPECT_EQ(WrittenValues.at(0), ElementValue);
  std::vector<std::uint64_t> WrittenTimes(1);
  TestWriter.Timestamp.read(WrittenTimes);
  EXPECT_EQ(WrittenTimes.at(0), Timestamp);
}

TEST_F(f144WriteData, WriteOneDefaultValueElement) {
  f144_WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);
  // 0 is the default value for a number in flatbuffers, so it doesn't actually
  // end up in buffer. We'll test this specifically, because it has
  // caused a bug in the past.
  double ElementValue{0.0};
  std::uint64_t Timestamp{11};
  auto FlatbufferData = f144_schema::generateFlatbufferMessage(ElementValue, Timestamp);
  EXPECT_EQ(TestWriter.Values.get_extent(), hdf5::Dimensions({0, 1}));
  EXPECT_EQ(TestWriter.Timestamp.dataspace().size(), 0);
  TestWriter.write(FileWriter::FlatbufferMessage(FlatbufferData.first.get(),
                                                 FlatbufferData.second));
  ASSERT_EQ(TestWriter.Values.get_extent(), hdf5::Dimensions({1, 1}));
  ASSERT_EQ(TestWriter.Timestamp.dataspace().size(), 1);
  std::vector<double> WrittenValues(1);
  TestWriter.Values.read(WrittenValues);
  EXPECT_EQ(WrittenValues.at(0), ElementValue);
  std::vector<std::uint64_t> WrittenTimes(1);
  TestWriter.Timestamp.read(WrittenTimes);
  EXPECT_EQ(WrittenTimes.at(0), Timestamp);
}
namespace f144_schema {
  std::pair<std::unique_ptr<uint8_t[]>, size_t>
  generateFlatbufferArrayMessage(std::vector<double> Value, uint64_t Timestamp) {
    auto ValueFunc = [Value](auto &Builder) {
      auto VectorOffset = Builder.CreateVector(Value);
      ArrayDoubleBuilder ValueBuilder(Builder);
      ValueBuilder.add_value(VectorOffset);
      return ValueBuilder.Finish().Union();
    };
    return generateFlatbufferMessageBase(ValueFunc, Value::ArrayDouble,
                                         Timestamp);
  }
}

TEST_F(f144WriteData, WriteOneArray) {
  f144_WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);
  std::vector<double> ElementValues{3.14, 4.5, 3.1};
  uint64_t Timestamp{12};
  auto FlatbufferData =
    f144_schema::generateFlatbufferArrayMessage(ElementValues, Timestamp);
  TestWriter.write(FileWriter::FlatbufferMessage(FlatbufferData.first.get(),
                                                 FlatbufferData.second));
  ASSERT_EQ(TestWriter.Values.get_extent(), hdf5::Dimensions({1, 3}));
  std::vector<double> WrittenValues(3);
  TestWriter.Values.read(WrittenValues);
  EXPECT_EQ(WrittenValues, ElementValues);
}

TEST_F(f144WriteData, WriteCueIndex) {
  f144_WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "cue_interval": 4
  })");
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);
  std::vector<double> ElementValues{3.14, 1.234};
  for (unsigned int i = 0; i < 10; ++i) {
    auto FlatbufferData = f144_schema::generateFlatbufferArrayMessage(ElementValues, i + 10);
    TestWriter.write(FileWriter::FlatbufferMessage(FlatbufferData.first.get(),
                                                   FlatbufferData.second));
  }
  ASSERT_EQ(TestWriter.CueIndex.size(), 2u);
  ASSERT_EQ(TestWriter.CueTimestampZero.size(), 2u);
  std::vector<uint32_t> WrittenCueIndices(2);
  std::vector<uint64_t> WrittenCueTimestamps(2);
  TestWriter.CueIndex.read(WrittenCueIndices);
  TestWriter.CueTimestampZero.read(WrittenCueTimestamps);
  std::vector<uint32_t> ExpectedIndices{3, 7};
  std::vector<uint64_t> ExpectedTimestamps{13, 17};
  EXPECT_EQ(WrittenCueIndices, ExpectedIndices);
  EXPECT_EQ(WrittenCueTimestamps, ExpectedTimestamps);
  std::vector<uint64_t> WrittenTimestamps(10);
  TestWriter.Timestamp.read(WrittenTimestamps);
  for (unsigned int j = 0; j < WrittenCueIndices.size(); j++) {
    EXPECT_EQ(WrittenCueTimestamps[j], WrittenTimestamps[WrittenCueIndices[j]]);
  }
}

TEST_F(f144WriteData, WriteNoCueIndex) {
  f144_WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "cue_interval": 11
  })");
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);
  std::vector<double> ElementValues{3.14, 1.234};
  for (unsigned int i = 0; i < 10; ++i) {
    auto FlatbufferData = f144_schema::generateFlatbufferArrayMessage(ElementValues, i + 10);
    TestWriter.write(FileWriter::FlatbufferMessage(FlatbufferData.first.get(),
                                                   FlatbufferData.second));
  }
  ASSERT_EQ(TestWriter.CueIndex.size(), 0u);
  ASSERT_EQ(TestWriter.CueTimestampZero.size(), 0u);
}

TEST_F(f144WriteData, WriteNoCueIndexAlt) {
  f144_WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "cue_interval": 1
  })");
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);
  ASSERT_EQ(TestWriter.CueIndex.size(), 0u);
  ASSERT_EQ(TestWriter.CueTimestampZero.size(), 0u);
}
