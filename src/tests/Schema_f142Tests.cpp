// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

// This filename is chosen such that it shows up in searches after the
// case-sensitive flatbuffer schema identifier.

#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>
#include <memory>

#include "FlatbufferMessage.h"
#include "helper.h"
#include "helpers/HDFFileTestHelper.h"
#include "schemas/f142/FlatbufferReader.h"
#include "schemas/f142/f142_rw.h"

using nlohmann::json;

using namespace FileWriter::Schemas::f142;

class f142Init : public ::testing::Test {
public:
  void SetUp() override {
    TestFile =
        HDFFileTestHelper::createInMemoryTestFile("SomeTestFile.hdf5", false);
    RootGroup = TestFile.H5File.root();
  }
  FileWriter::HDFFile TestFile;
  hdf5::node::Group RootGroup;
};

class f142WriterStandIn : public f142Writer {
public:
  using f142Writer::ArraySize;
  using f142Writer::ChunkSize;
  using f142Writer::CueIndex;
  using f142Writer::CueTimestampZero;
  using f142Writer::ElementType;
  using f142Writer::Timestamp;
  using f142Writer::ValueIndexInterval;
  using f142Writer::Values;
};

TEST_F(f142Init, BasicDefaultInit) {
  f142Writer TestWriter;
  TestWriter.init_hdf(RootGroup, "");
  EXPECT_TRUE(RootGroup.has_dataset("cue_index"));
  EXPECT_TRUE(RootGroup.has_dataset("cue_timestamp_zero"));
  EXPECT_TRUE(RootGroup.has_dataset("time"));
  EXPECT_TRUE(RootGroup.has_dataset("value"));
}

TEST_F(f142Init, ReOpenSuccess) {
  f142Writer TestWriter;
  TestWriter.init_hdf(RootGroup, "");
  EXPECT_EQ(TestWriter.reopen(RootGroup), f142Writer::InitResult::OK);
}

TEST_F(f142Init, ReOpenFailure) {
  f142Writer TestWriter;
  EXPECT_EQ(TestWriter.reopen(RootGroup), f142Writer::InitResult::ERROR);
}

TEST_F(f142Init, CheckInitDataType) {
  f142WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup, "");
  auto Open = NeXusDataset::Mode::Open;
  NeXusDataset::MultiDimDatasetBase Value(RootGroup, Open);
  EXPECT_EQ(Value.datatype(), hdf5::datatype::create<double>());
}

TEST_F(f142Init, CheckValueInitShape1) {
  f142WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup, "");
  auto Open = NeXusDataset::Mode::Open;
  NeXusDataset::MultiDimDatasetBase Value(RootGroup, Open);
  EXPECT_EQ(hdf5::Dimensions({0, 1}), Value.get_extent());
}

TEST_F(f142Init, CheckValueInitShape2) {
  f142WriterStandIn TestWriter;
  TestWriter.ArraySize = 10;
  TestWriter.init_hdf(RootGroup, "");
  auto Open = NeXusDataset::Mode::Open;
  NeXusDataset::MultiDimDatasetBase Value(RootGroup, Open);
  EXPECT_EQ(hdf5::Dimensions({0, 10}), Value.get_extent());
}

TEST_F(f142Init, CheckAllDataTypes) {
  std::vector<std::pair<f142Writer::Type, hdf5::datatype::Datatype>> TypeMap{
      {f142Writer::Type::int8, hdf5::datatype::create<std::int8_t>()},
      {f142Writer::Type::uint8, hdf5::datatype::create<std::uint8_t>()},
      {f142Writer::Type::int16, hdf5::datatype::create<std::int16_t>()},
      {f142Writer::Type::uint16, hdf5::datatype::create<std::uint16_t>()},
      {f142Writer::Type::int32, hdf5::datatype::create<std::int32_t>()},
      {f142Writer::Type::uint32, hdf5::datatype::create<std::uint32_t>()},
      {f142Writer::Type::int64, hdf5::datatype::create<std::int64_t>()},
      {f142Writer::Type::uint64, hdf5::datatype::create<std::uint64_t>()},
      {f142Writer::Type::float32, hdf5::datatype::create<float>()},
      {f142Writer::Type::float64, hdf5::datatype::create<double>()}};
  auto Open = NeXusDataset::Mode::Open;
  f142WriterStandIn TestWriter;
  int Ctr{0};
  for (auto &Type : TypeMap) {
    auto CurrentGroup = RootGroup.create_group("Group" + std::to_string(Ctr++));
    TestWriter.ElementType = Type.first;
    TestWriter.init_hdf(CurrentGroup, "");
    NeXusDataset::MultiDimDatasetBase Value(CurrentGroup, Open);
    EXPECT_EQ(Type.second, Value.datatype());
  }
}

class f142ConfigParse : public ::testing::Test {
public:
};

TEST_F(f142ConfigParse, EmptyConfig) {
  f142WriterStandIn TestWriter;
  TestWriter.parse_config("{}");
  f142WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ElementType, TestWriter2.ElementType);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f142ConfigParse, SetArraySize) {
  f142WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "array_size": 3
            })");
  f142WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, 3u);
  EXPECT_EQ(TestWriter.ElementType, TestWriter2.ElementType);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f142ConfigParse, SetChunkSize) {
  f142WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "nexus.chunk_size": 511
            })");
  f142WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ElementType, TestWriter2.ElementType);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ChunkSize, 511u);
}

TEST_F(f142ConfigParse, CuInterval) {
  f142WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "nexus.cue_interval": 24
            })");
  f142WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ElementType, TestWriter2.ElementType);
  EXPECT_EQ(TestWriter.ValueIndexInterval, 24u);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f142ConfigParse, DataType1) {
  f142WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "type": "int8"
            })");
  f142WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ElementType, f142Writer::Type::int8);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f142ConfigParse, DataType2) {
  f142WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "dtype": "uint64"
            })");
  f142WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ElementType, f142Writer::Type::uint64);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f142ConfigParse, DataTypeFailure) {
  f142WriterStandIn TestWriter;
  TestWriter.parse_config(R"({
              "Dtype": "uint64"
            })");
  f142WriterStandIn TestWriter2;
  EXPECT_EQ(TestWriter.ArraySize, TestWriter2.ArraySize);
  EXPECT_EQ(TestWriter.ElementType, f142Writer::Type::float64);
  EXPECT_EQ(TestWriter.ValueIndexInterval, TestWriter2.ValueIndexInterval);
  EXPECT_EQ(TestWriter.ChunkSize, TestWriter2.ChunkSize);
}

TEST_F(f142ConfigParse, DataTypes) {
  using Type = f142Writer::Type;
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
    f142WriterStandIn TestWriter;
    EXPECT_EQ(TestWriter.ElementType, Type::float64);
    TestWriter.parse_config("{\"type\":\"" + CType.first + "\"}");
    EXPECT_EQ(TestWriter.ElementType, CType.second) << "Failed on type string: "
                                                    << CType.first;
  }
}

class f142WriteData : public ::testing::Test {
public:
  void SetUp() override {
    TestFile =
        HDFFileTestHelper::createInMemoryTestFile("SomeTestFile.hdf5", false);
    RootGroup = TestFile.H5File.root();
  }
  FileWriter::HDFFile TestFile;
  hdf5::node::Group RootGroup;
};

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

TEST_F(f142WriteData, ConfigUnitsAttributeOnValueDataset) {
  f142WriterStandIn TestWriter;
  const std::string units_string = "parsecs";
  // GIVEN value_units is specified in the JSON config
  TestWriter.parse_config(
      fmt::format(R"({{"value_units": "{}"}})", units_string));

  // WHEN the writer module creates the datasets
  TestWriter.init_hdf(RootGroup, "");
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

TEST_F(f142WriteData, ConfigUnitsAttributeOnValueDatasetIfEmpty) {
  f142WriterStandIn TestWriter;
  // GIVEN value_units is specified as an empty string in the JSON config
  TestWriter.parse_config(R"({"value_units": ""})");

  // WHEN the writer module creates the datasets
  TestWriter.init_hdf(RootGroup, "");
  TestWriter.reopen(RootGroup);

  // THEN a units attributes is created on the value dataset with an empty
  // string value
  std::string attribute_value;
  EXPECT_NO_THROW(TestWriter.Values.attributes["units"].read(attribute_value))
      << "Expect units attribute to be present on the value dataset";
  EXPECT_EQ(attribute_value, "") << "Expect units attribute to have empty "
                                    "string as the value, as specified in the "
                                    "JSON configuration";
}

TEST_F(f142WriteData, UnitsAttributeOnValueDatasetNotCreatedIfNotInConfig) {
  f142WriterStandIn TestWriter;
  // GIVEN value_units is not specified in the JSON config
  TestWriter.parse_config("{}");

  // WHEN the writer module creates the datasets
  TestWriter.init_hdf(RootGroup, "");
  TestWriter.reopen(RootGroup);

  // THEN a units attributes is not created on the value dataset
  EXPECT_FALSE(TestWriter.Values.attributes.exists("units"))
      << "units attribute should not be created if it was not specified in the "
         "JSON config";
}

TEST_F(f142WriteData, WriteOneElement) {
  f142WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup, "");
  TestWriter.reopen(RootGroup);
  double ElementValue{3.14};
  std::uint64_t Timestamp{11};
  auto FlatbufferData = generateFlatbufferMessage(ElementValue, Timestamp);
  EXPECT_EQ(TestWriter.Values.get_extent(), hdf5::Dimensions({0, 1}));
  EXPECT_EQ(TestWriter.Timestamp.dataspace().size(), 0);
  TestWriter.write(FileWriter::FlatbufferMessage(
      reinterpret_cast<char const *>(FlatbufferData.first.get()),
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

std::pair<std::unique_ptr<uint8_t[]>, size_t>
generateFlatbufferArrayMessage(std::vector<double> Value,
                               std::uint64_t Timestamp) {
  auto ValueFunc = [Value](auto &Builder) {
    auto VectorOffset = Builder.CreateVector(Value);
    ArrayDoubleBuilder ValueBuilder(Builder);
    ValueBuilder.add_value(VectorOffset);
    return ValueBuilder.Finish().Union();
  };
  return generateFlatbufferMessageBase(ValueFunc, Value::ArrayDouble,
                                       Timestamp);
}

TEST_F(f142WriteData, WriteOneArray) {
  f142WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup, "");
  TestWriter.reopen(RootGroup);
  std::vector<double> ElementValues{3.14, 4.5, 3.1};
  std::uint64_t Timestamp{12};
  auto FlatbufferData =
      generateFlatbufferArrayMessage(ElementValues, Timestamp);
  TestWriter.write(FileWriter::FlatbufferMessage(
      reinterpret_cast<char const *>(FlatbufferData.first.get()),
      FlatbufferData.second));
  ASSERT_EQ(TestWriter.Values.get_extent(), hdf5::Dimensions({1, 3}));
  std::vector<double> WrittenValues(3);
  TestWriter.Values.read(WrittenValues);
  EXPECT_EQ(WrittenValues, ElementValues);
}
