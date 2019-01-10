// This filename is chosen such that it shows up in searches after the
// case-sensitive flatbuffer schema identifier.

#include "AddReader.h"
#include "CommandHandler.h"
#include "FlatbufferMessage.h"
#include "MainOpt.h"
#include "Msg.h"
#include "helper.h"
#include "json.h"
#include "schemas/f142/FlatbufferReader.h"
#include "schemas/f142/f142_rw.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>
#include <memory>

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
makeArrayDouble(std::string SourceName, uint64_t Timestamp,
                std::vector<double> Value) {
  auto BuilderPtr = std::make_unique<flatbuffers::FlatBufferBuilder>();
  auto &Builder = *BuilderPtr;
  auto SourceNameOffset = Builder.CreateString(SourceName);
  auto ValueOffset = Builder.CreateVector(Value);
  auto ValueType = FileWriter::Schemas::f142::Value::ArrayDouble;
  FileWriter::Schemas::f142::ArrayDoubleBuilder ValueBuilder(Builder);
  ValueBuilder.add_value(ValueOffset);
  auto UnionOffset = ValueBuilder.Finish().Union();
  FileWriter::Schemas::f142::LogDataBuilder LogDataBuilder(Builder);
  LogDataBuilder.add_value_type(ValueType);
  LogDataBuilder.add_value(UnionOffset);
  LogDataBuilder.add_source_name(SourceNameOffset);
  LogDataBuilder.add_timestamp(Timestamp);
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
  auto File = createInMemoryTestFile("Schema_f142.writeScalarFloat");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.root();
    WriterModule.init_hdf(Group, "{}");
    for (auto const &Value : Expected) {
      auto Builder = makeValueFloat(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          reinterpret_cast<char *>(Builder->GetBufferPointer()),
          Builder->GetSize());
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
  auto File = createInMemoryTestFile("Schema_f142.writeArrayFloat");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.root();
    WriterModule.init_hdf(Group, "{}");
    for (auto const &Value : Expected) {
      auto Builder = makeValue(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          reinterpret_cast<char *>(Builder->GetBufferPointer()),
          Builder->GetSize());
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

TEST_F(Schema_f142, writeScalarString) {
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
    for (auto const &Value : Expected) {
      auto Builder = makeValueString(Value);
      auto Msg = FileWriter::FlatbufferMessage(
          reinterpret_cast<char *>(Builder->GetBufferPointer()),
          Builder->GetSize());
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

TEST_F(Schema_f142, writeScalarFloatWithLatestWithoutContent) {
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "store_latest_into": "the_latest_value",
    "type": "float"
  })"");
  auto File = createInMemoryTestFile(
      "Schema_f142.writeScalarFloatWithLatestWithoutContent");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.root();
    WriterModule.init_hdf(Group, "{}");
    WriterModule.close();
  }

  ASSERT_TRUE(File.root().has_dataset("value"));
  ASSERT_TRUE(File.root().has_dataset("time"));
  ASSERT_TRUE(File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.root().has_dataset("cue_index"));
  ASSERT_FALSE(File.root().has_dataset("the_latest_value"));
}

TEST_F(Schema_f142, writeArrayFloatWithLatestWithoutContent) {
  auto StreamJson = json::parse(R""({
    "source": "the_source_01",
    "writer_module": "f142",
    "store_latest_into": "the_latest_value",
    "type": "float",
    "array_size": 5
  })"");
  auto File = createInMemoryTestFile(
      "Schema_f142.writeArrayFloatWithLatestWithoutContent");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.root();
    WriterModule.init_hdf(Group, "{}");
    WriterModule.close();
  }

  ASSERT_TRUE(File.root().has_dataset("value"));
  ASSERT_TRUE(File.root().has_dataset("time"));
  ASSERT_TRUE(File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.root().has_dataset("cue_index"));
  ASSERT_FALSE(File.root().has_dataset("the_latest_value"));
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
  auto File = createInMemoryTestFile("Schema_f142.writeScalarFloatWithLatest");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.root();
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

  ASSERT_TRUE(File.root().has_dataset("value"));
  ASSERT_TRUE(File.root().has_dataset("time"));
  ASSERT_TRUE(File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.root().has_dataset("cue_index"));
  ASSERT_TRUE(File.root().has_dataset("the_latest_value"));

  auto Dataset = hdf5::node::get_dataset(File.root(), "value");
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
  auto File =
      createInMemoryTestFile("Schema_f142.writeArrayFloatWithLatest", true);
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.parse_config(StreamJson.dump(), "{}");
    auto Group = File.root();
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

  ASSERT_TRUE(File.root().has_dataset("value"));
  ASSERT_TRUE(File.root().has_dataset("time"));
  ASSERT_TRUE(File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.root().has_dataset("cue_index"));
  ASSERT_TRUE(File.root().has_dataset("the_latest_value"));

  size_t const N = Expected.at(0).size();
  ASSERT_EQ(5u, N);
  std::vector<DT> Data(N);
  {
    auto Dataset = hdf5::node::get_dataset(File.root(), "value");
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
    auto Dataset = hdf5::node::get_dataset(File.root(), "the_latest_value");
    std::vector<DT> LatestData(N);
    auto SpaceMem = hdf5::dataspace::Simple({1, N});
    auto SpaceFile = hdf5::dataspace::Simple(Dataset.dataspace());
    SpaceFile.selection(hdf5::dataspace::SelectionOperation::SET,
                        hdf5::dataspace::Hyperslab({0}, {N}));
    Dataset.read(LatestData, Dataset.datatype(), SpaceMem, SpaceFile);
    ASSERT_EQ(LatestData, Expected.back());
  }
}

TEST_F(Schema_f142, UninitializedStreamsDoNotGetReopenedOnStartOfWriting) {
  using FileWriter::CommandHandler;
  using FileWriter::FlatbufferMessage;
  std::string Filename(
      "Test.Schema_f142.UninitializedStreamsDoNotGetReopenedOnStartOfWriting");
  unlink(Filename.c_str());
  MainOpt MainOpt;
  CommandHandler CommandHandler(MainOpt, nullptr);
  auto Command = json::parse(R""(
{
  "cmd": "FileWriter_new",
  "file_attributes": {
    "file_name": "tmp-dummy-hdf"
  },
  "job_id": "dummy",
  "broker": "//localhost:202020",
  "nexus_structure": {
    "children": [
      {
        "name": "some_nxlog",
        "type": "group",
        "children": [
          {
            "type": "stream",
            "stream": {
              "writer_module": "f142",
              "topic": "dummy_topic",
              "source": "dummy_source_1",
              "type": "double",
              "array_size": 3
            }
          },
          {
            "type": "stream",
            "stream": {
              "writer_module": "f142",
              "topic": "dummy_topic",
              "source": "dummy_source_2",
              "type": "double",
              "array_size": 3
            }
          }
        ]
      }
    ]
  }
}
  )"");
  Command["file_attributes"]["file_name"] = Filename;
  Command["job_id"] = Filename;
  auto CommandString = Command.dump();
  CommandHandler.tryToHandle(CommandString);

  // We write to both sources, but afterwards verify that only the data to the
  // first source has been written
  // to HDF, while data for the other colliding source has been discarded.

  ASSERT_EQ(
      CommandHandler.getFileWriterTaskByJobID(Filename)->demuxers().size(), 1u);
  auto &Demux =
      CommandHandler.getFileWriterTaskByJobID(Filename)->demuxers().at(0);
  uint64_t Timestamp = 42;
  auto toMessage =
      [](std::unique_ptr<flatbuffers::FlatBufferBuilder> const &Builder) {
        return FlatbufferMessage(
            reinterpret_cast<char const *>(Builder->GetBufferPointer()),
            Builder->GetSize());
      };
  Demux.process_message(
      toMessage(makeArrayDouble("dummy_source_2", Timestamp, {1, 2, 3})));
  Demux.process_message(
      toMessage(makeArrayDouble("dummy_source_1", Timestamp, {4, 5, 6})));
  Demux.process_message(
      toMessage(makeArrayDouble("dummy_source_2", Timestamp, {7, 8, 9})));
  Demux.process_message(
      toMessage(makeArrayDouble("dummy_source_1", Timestamp, {10, 11, 12})));

  auto CommandStop = json::parse(R""(
{
  "cmd": "file_writer_tasks_clear_all",
  "recv_type": "FileWriter"
}
  )"");
  CommandString = CommandStop.dump();
  CommandHandler.tryToHandle(CommandString);
  {
    auto File = hdf5::file::open(Filename);
    File.root().get_group("some_nxlog").get_dataset("value");
    std::vector<double> Buffer(6);
    File.root()
        .get_group("some_nxlog")
        .get_dataset("value")
        .read(Buffer, hdf5::property::DatasetTransferList());
    ASSERT_EQ(Buffer, std::vector<double>({4, 5, 6, 10, 11, 12}));
  }
  unlink(Filename.c_str());
}
