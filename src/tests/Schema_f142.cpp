// This filename is chosen such that it shows up in searches after the
// case-sensitive flatbuffer schema identifier.

#include "../Msg.h"
#include "../helper.h"
#include "../schemas/f142/f142_rw.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>
#include <memory>

hdf5::file::File createInMemoryTestFile(std::string const &Filename) {
  hdf5::property::FileCreationList FCPL;
  hdf5::property::FileAccessList FAPL;
  FAPL.driver(hdf5::file::MemoryDriver());
  return hdf5::file::create(Filename, hdf5::file::AccessFlags::TRUNCATE, FCPL,
                            FAPL);
}

static std::unique_ptr<flatbuffers::FlatBufferBuilder> makeFlatbuffer() {
  auto BuilderPtr = make_unique<flatbuffers::FlatBufferBuilder>();
  auto &Builder = *BuilderPtr;
  FileWriter::Schemas::f142::FloatBuilder FloatBuilder(Builder);
  FloatBuilder.add_value(0.125);
  auto FloatOffset = FloatBuilder.Finish().Union();
  FileWriter::Schemas::f142::LogDataBuilder LogDataBuilder(Builder);
  LogDataBuilder.add_value(FloatOffset);
  LogDataBuilder.add_value_type(FileWriter::Schemas::f142::Value::Float);
  FileWriter::Schemas::f142::FinishLogDataBuffer(Builder,
                                                 LogDataBuilder.Finish());
  return std::move(BuilderPtr);
}

TEST(Schema_f142, basic) {
  size_t N = 16;
  auto File = createInMemoryTestFile("tmp-dummy.h5");
  {
    FileWriter::Schemas::f142::HDFWriterModule WriterModule;
    WriterModule.TypeName = "float";
    WriterModule.ArraySize = 0;
    auto Group = File.root();
    WriterModule.init_hdf(Group, "{}");
    auto Builder = makeFlatbuffer();
    for (size_t i = 0; i < N; ++i) {
      auto Msg = FileWriter::Msg::owned(
          reinterpret_cast<char *>(Builder->GetBufferPointer()),
          Builder->GetSize());
      WriterModule.write(std::move(Msg));
    }
  }
  ASSERT_TRUE(File.root().has_dataset("value"));
  ASSERT_TRUE(File.root().has_dataset("time"));
  ASSERT_TRUE(File.root().has_dataset("cue_timestamp_zero"));
  ASSERT_TRUE(File.root().has_dataset("cue_index"));

  auto Dataset = hdf5::node::get_dataset(File.root(), "value");
  std::vector<float> Data;
  Data.resize(N);
  hdf5::dataspace::Simple SpaceFile({N});
  hdf5::dataspace::Simple SpaceMem({N});
  SpaceFile.selection.all();
  SpaceMem.selection.all();
  Dataset.read(*Data.data(), Dataset.datatype(), SpaceFile, SpaceMem,
               hdf5::property::DatasetTransferList());
  for (size_t i = 0; i < N; ++i) {
    ASSERT_EQ(Data.at(i), 0.125);
  }
}
