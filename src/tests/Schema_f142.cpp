// This filename is chosen such that it shows up in searches after the
// case-sensitive flatbuffer schema identifier.

#include "../Msg.h"
#include "../helper.h"
#include "../json.h"
#include "../schemas/f142/f142_rw.h"
#include "AddReader.h"
#include "FlatbufferMessage.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>
#include <memory>

using nlohmann::json;

hdf5::file::File createInMemoryTestFile(std::string const &Filename,
                                        bool OnDisk = false) {
  hdf5::property::FileCreationList FCPL;
  hdf5::property::FileAccessList FAPL;
  if (!OnDisk) {
    FAPL.driver(hdf5::file::MemoryDriver());
  }
  return hdf5::file::create(Filename, hdf5::file::AccessFlags::TRUNCATE, FCPL,
                            FAPL);
}

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

TEST(Schema_f142, writeScalarFloat) {
  AddF142Reader();
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "type": "float"
  })"");
  std::vector<float> Expected{0.125, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0};
  auto File = createInMemoryTestFile("Schema_f142.writeScalarFloat");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.root();
    WriterModule.init_hdf(Group, "{}");
    for (auto Value : Expected) {
      auto Builder = makeValueFloat(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          (char *)Builder->GetBufferPointer(), Builder->GetSize());
      WriterModule.write(Msg);
    }
  }
  ASSERT_TRUE(File.root().has_dataset("value"));
  ASSERT_TRUE(File.root().has_dataset("time"));
  ASSERT_TRUE(File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.root().has_dataset("cue_index"));

  auto Dataset = hdf5::node::get_dataset(File.root(), "value");
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

TEST(Schema_f142, writeArrayFloat) {
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
  auto File = createInMemoryTestFile("Schema_f142.writeArrayFloat", true);
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.root();
    WriterModule.init_hdf(Group, "{}");
    for (auto Value : Expected) {
      auto Builder = makeValue(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          (char *)Builder->GetBufferPointer(), Builder->GetSize());
      WriterModule.write(Msg);
    }
  }
  ASSERT_TRUE(File.root().has_dataset("value"));
  ASSERT_TRUE(File.root().has_dataset("time"));
  ASSERT_TRUE(File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.root().has_dataset("cue_index"));

  auto Dataset = hdf5::node::get_dataset(File.root(), "value");
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

TEST(Schema_f142, writeScalarString) {
  AddF142Reader();
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "type": "string"
  })"");
  std::vector<std::string> Expected{"string0", "string1", "string2"};
  auto File = createInMemoryTestFile("Tests.Schema_f142.writeScalarString");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.root();
    WriterModule.init_hdf(Group, "{}");
    for (auto Value : Expected) {
      auto Builder = makeValueString(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          (char *)Builder->GetBufferPointer(), Builder->GetSize());
      WriterModule.write(Msg);
    }
  }
  ASSERT_TRUE(File.root().has_dataset("value"));
  ASSERT_TRUE(File.root().has_dataset("time"));
  ASSERT_TRUE(File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.root().has_dataset("cue_index"));

  auto Dataset = hdf5::node::get_dataset(File.root(), "value");
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
