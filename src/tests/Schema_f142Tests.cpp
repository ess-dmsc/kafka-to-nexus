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

#include "AddReader.h"
#include "CommandHandler.h"
#include "FlatbufferMessage.h"
#include "MainOpt.h"
#include "helper.h"
#include "helpers/HDFFileTestHelper.h"
#include "json.h"
#include "schemas/f142/FlatbufferReader.h"
#include "schemas/f142/f142_rw.h"

using nlohmann::json;

class Schema_f142 : public ::testing::Test {
public:
  void SetUp() override {
    try {
      FileWriter::FlatbufferReaderRegistry::Registrar<
          FileWriter::Schemas::f142::FlatbufferReader>
          RegisterIt("f142");
    } catch (...) {
    }
    try {
      FileWriter::HDFWriterModuleRegistry::Registrar<
          FileWriter::Schemas::f142::HDFWriterModule>
          RegisterIt("f142");
    } catch (...) {
    }
  }
};

static std::unique_ptr<flatbuffers::FlatBufferBuilder>
makeValueFloat(float Value) {
  auto BuilderPtr = std::make_unique<flatbuffers::FlatBufferBuilder>();
  auto &Builder = *BuilderPtr;
  FileWriter::Schemas::f142::FloatBuilder FloatBuilder(Builder);
  FloatBuilder.add_value(Value);
  auto FloatOffset = FloatBuilder.Finish().Union();
  FileWriter::Schemas::f142::LogDataBuilder LogDataBuilder(Builder);
  LogDataBuilder.add_value(FloatOffset);
  LogDataBuilder.add_value_type(FileWriter::Schemas::f142::Value::Float);
  FileWriter::Schemas::f142::FinishLogDataBuffer(Builder,
                                                 LogDataBuilder.Finish());
  return BuilderPtr;
}

static std::unique_ptr<flatbuffers::FlatBufferBuilder>
makeValue(std::vector<float> Value) {
  auto BuilderPtr = std::make_unique<flatbuffers::FlatBufferBuilder>();
  auto &Builder = *BuilderPtr;
  auto ValueOffset = Builder.CreateVector(Value);
  FileWriter::Schemas::f142::ArrayFloatBuilder ValueBuilder(Builder);
  ValueBuilder.add_value(ValueOffset);
  auto UnionOffset = ValueBuilder.Finish().Union();
  FileWriter::Schemas::f142::LogDataBuilder LogDataBuilder(Builder);
  LogDataBuilder.add_value(UnionOffset);
  LogDataBuilder.add_value_type(FileWriter::Schemas::f142::Value::ArrayFloat);
  FileWriter::Schemas::f142::FinishLogDataBuffer(Builder,
                                                 LogDataBuilder.Finish());
  return BuilderPtr;
}

static std::unique_ptr<flatbuffers::FlatBufferBuilder>
makeValueString(std::string Value) {
  auto BuilderPtr = std::make_unique<flatbuffers::FlatBufferBuilder>();
  auto &Builder = *BuilderPtr;
  auto ValueOffset = Builder.CreateString(Value);
  FileWriter::Schemas::f142::StringBuilder ValueBuilder(Builder);
  ValueBuilder.add_value(ValueOffset);
  auto UnionOffset = ValueBuilder.Finish().Union();
  FileWriter::Schemas::f142::LogDataBuilder LogDataBuilder(Builder);
  LogDataBuilder.add_value(UnionOffset);
  LogDataBuilder.add_value_type(FileWriter::Schemas::f142::Value::String);
  FileWriter::Schemas::f142::FinishLogDataBuffer(Builder,
                                                 LogDataBuilder.Finish());
  return BuilderPtr;
}

TEST_F(Schema_f142, writeScalarFloat) {
  AddF142Reader();
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "type": "float"
  })"");
  std::vector<float> Expected{0.125, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0};
  auto File =
      HDFFileTestHelper::createInMemoryTestFile("Schema_f142.writeScalarFloat");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.H5File.root();
    WriterModule.init_hdf(Group, "{}");
    for (auto const &Value : Expected) {
      auto Builder = makeValueFloat(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          reinterpret_cast<char *>(Builder->GetBufferPointer()),
          Builder->GetSize());
      WriterModule.write(Msg);
    }
  }
  ASSERT_TRUE(File.H5File.root().has_dataset("value"));
  ASSERT_TRUE(File.H5File.root().has_dataset("time"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_index"));

  auto Dataset = hdf5::node::get_dataset(File.H5File.root(), "value");
  std::vector<float> Data;
  size_t N = Expected.size();
  Data.resize(N);
  hdf5::dataspace::Simple SpaceFile({N});
  hdf5::dataspace::Simple SpaceMem({N});
  SpaceFile.selection.all();
  SpaceMem.selection.all();
  Dataset.read(*Data.data(), Dataset.datatype(), SpaceMem, SpaceFile);
  for (size_t i = 0; i < N; ++i) {
    ASSERT_EQ(Data.at(i), Expected.at(i));
  }
}

TEST_F(Schema_f142, writeArrayFloat) {
  AddF142Reader();
  using DT = float;
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "type": "float",
    "array_size": 5
  })"");
  std::vector<std::vector<DT>> Expected{
      {4, 8, 1, 9, 4}, {5, 7, 0, 3, 6}, {4, 2, 2, 4, 2},
  };
  auto File =
      HDFFileTestHelper::createInMemoryTestFile("Schema_f142.writeArrayFloat");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.H5File.root();
    WriterModule.init_hdf(Group, "{}");
    for (auto const &Value : Expected) {
      auto Builder = makeValue(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          reinterpret_cast<char *>(Builder->GetBufferPointer()),
          Builder->GetSize());
      WriterModule.write(Msg);
    }
  }
  ASSERT_TRUE(File.H5File.root().has_dataset("value"));
  ASSERT_TRUE(File.H5File.root().has_dataset("time"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_index"));

  auto Dataset = hdf5::node::get_dataset(File.H5File.root(), "value");
  for (size_t I = 0; I < Expected.size(); ++I) {
    size_t N = Expected.at(0).size();
    std::vector<DT> Data(N);
    hdf5::dataspace::Simple SpaceFile(Dataset.dataspace());
    hdf5::dataspace::Simple SpaceMem({1, N});
    SpaceFile.selection(hdf5::dataspace::SelectionOperation::SET,
                        hdf5::dataspace::Hyperslab({I, 0}, {1, N}));
    Dataset.read(Data, Dataset.datatype(), SpaceMem, SpaceFile);
    ASSERT_EQ(Data, Expected.at(I));
  }
}

TEST_F(Schema_f142, writeScalarString) {
  AddF142Reader();
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "type": "string"
  })"");
  std::vector<std::string> Expected{"string0", "string1", "string2"};
  auto File = HDFFileTestHelper::createInMemoryTestFile(
      "Tests.Schema_f142.writeScalarString");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.H5File.root();
    WriterModule.init_hdf(Group, "{}");
    for (auto const &Value : Expected) {
      auto Builder = makeValueString(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          reinterpret_cast<char *>(Builder->GetBufferPointer()),
          Builder->GetSize());
      WriterModule.write(Msg);
    }
  }
  ASSERT_TRUE(File.H5File.root().has_dataset("value"));
  ASSERT_TRUE(File.H5File.root().has_dataset("time"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_index"));

  auto Dataset = hdf5::node::get_dataset(File.H5File.root(), "value");
  std::vector<std::string> Data;
  size_t N = Expected.size();
  Data.resize(N);
  hdf5::dataspace::Simple SpaceFile({N});
  hdf5::dataspace::Simple SpaceMem({N});
  SpaceFile.selection.all();
  SpaceMem.selection.all();
  Dataset.read(Data, Dataset.datatype(), SpaceMem, SpaceFile);
  for (size_t I = 0; I < N; ++I) {
    ASSERT_EQ(Data.at(I), Expected.at(I));
  }
}

TEST_F(Schema_f142, writeScalarFloatWithLatestWithoutContent) {
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "store_latest_into": "the_latest_value",
    "type": "float"
  })"");
  auto File = HDFFileTestHelper::createInMemoryTestFile(
      "Schema_f142.writeScalarFloatWithLatestWithoutContent");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.H5File.root();
    WriterModule.init_hdf(Group, "{}");
    WriterModule.close();
  }

  ASSERT_TRUE(File.H5File.root().has_dataset("value"));
  ASSERT_TRUE(File.H5File.root().has_dataset("time"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_index"));
  ASSERT_FALSE(File.H5File.root().has_dataset("the_latest_value"));
}

TEST_F(Schema_f142, writeArrayFloatWithLatestWithoutContent) {
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "store_latest_into": "the_latest_value",
    "type": "float",
    "array_size": 5
  })"");
  auto File = HDFFileTestHelper::createInMemoryTestFile(
      "Schema_f142.writeArrayFloatWithLatestWithoutContent");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.H5File.root();
    WriterModule.init_hdf(Group, "{}");
    WriterModule.close();
  }

  ASSERT_TRUE(File.H5File.root().has_dataset("value"));
  ASSERT_TRUE(File.H5File.root().has_dataset("time"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_index"));
  ASSERT_FALSE(File.H5File.root().has_dataset("the_latest_value"));
}

TEST_F(Schema_f142, writeScalarFloatWithLatest) {
  using DT = float;
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "store_latest_into": "the_latest_value",
    "type": "float"
  })"");
  std::vector<DT> Expected{7, 8, 9};
  auto File = HDFFileTestHelper::createInMemoryTestFile(
      "Schema_f142.writeScalarFloatWithLatest");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.H5File.root();
    WriterModule.init_hdf(Group, "{}");
    for (auto const &Value : Expected) {
      auto Builder = makeValueFloat(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          reinterpret_cast<char const *>(Builder->GetBufferPointer()),
          Builder->GetSize());
      WriterModule.write(Msg);
    }
    WriterModule.close();
  }

  ASSERT_TRUE(File.H5File.root().has_dataset("value"));
  ASSERT_TRUE(File.H5File.root().has_dataset("time"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_index"));
  ASSERT_TRUE(File.H5File.root().has_dataset("the_latest_value"));

  auto Dataset = hdf5::node::get_dataset(File.H5File.root(), "value");
  for (size_t I = 0; I < Expected.size(); ++I) {
    DT Data;
    hdf5::dataspace::Simple SpaceFile(Dataset.dataspace());
    hdf5::dataspace::Simple SpaceMem({1});
    SpaceFile.selection(hdf5::dataspace::SelectionOperation::SET,
                        hdf5::dataspace::Hyperslab({I}, {1}));
    Dataset.read(Data, Dataset.datatype(), SpaceMem, SpaceFile);
    ASSERT_EQ(Data, Expected.at(I));
  }
}

TEST_F(Schema_f142, writeArrayFloatWithLatest) {
  using DT = float;
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "store_latest_into": "the_latest_value",
    "type": "float",
    "array_size": 5
  })"");
  std::vector<std::vector<DT>> Expected{
      {4, 8, 1, 9, 4}, {5, 7, 0, 3, 6}, {4, 2, 2, 4, 2},
  };
  auto File = HDFFileTestHelper::createInMemoryTestFile(
      "Schema_f142.writeArrayFloatWithLatest", true);
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.H5File.root();
    WriterModule.init_hdf(Group, "{}");
    for (auto const &Value : Expected) {
      auto Builder = makeValue(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          reinterpret_cast<char const *>(Builder->GetBufferPointer()),
          Builder->GetSize());
      WriterModule.write(Msg);
    }
    WriterModule.close();
  }

  ASSERT_TRUE(File.H5File.root().has_dataset("value"));
  ASSERT_TRUE(File.H5File.root().has_dataset("time"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.H5File.root().has_dataset("cue_index"));
  ASSERT_TRUE(File.H5File.root().has_dataset("the_latest_value"));

  size_t const N = Expected.at(0).size();
  ASSERT_EQ(5u, N);
  std::vector<DT> Data(N);
  {
    auto Dataset = hdf5::node::get_dataset(File.H5File.root(), "value");
    for (size_t I = 0; I < Expected.size(); ++I) {
      hdf5::dataspace::Simple SpaceMem({1, N});
      hdf5::dataspace::Simple SpaceFile(Dataset.dataspace());
      SpaceFile.selection(hdf5::dataspace::SelectionOperation::SET,
                          hdf5::dataspace::Hyperslab({I, 0}, {1, N}));
      Dataset.read(Data, Dataset.datatype(), SpaceMem, SpaceFile);
      ASSERT_EQ(Data, Expected.at(I));
    }
  }

  {
    auto Dataset =
        hdf5::node::get_dataset(File.H5File.root(), "the_latest_value");
    std::vector<DT> LatestData(N);
    auto SpaceMem = hdf5::dataspace::Simple({1, N});
    auto SpaceFile = hdf5::dataspace::Simple(Dataset.dataspace());
    SpaceFile.selection(hdf5::dataspace::SelectionOperation::SET,
                        hdf5::dataspace::Hyperslab({0}, {N}));
    Dataset.read(LatestData, Dataset.datatype(), SpaceMem, SpaceFile);
    ASSERT_EQ(LatestData, Expected.back());
  }
}
