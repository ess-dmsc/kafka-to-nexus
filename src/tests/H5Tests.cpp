#include "../h5.h"
#include "schemas/f142/f142_rw.h"
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>
#include <string>
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace f142 {
#include "schemas/f142_logdata_generated.h"
} // namespace f142
} // namespace Schemas
} // namespace FileWriter

hdf5::file::File createInMemoryFile() {
  hdf5::property::FileAccessList fapl;
  fapl.driver(hdf5::file::MemoryDriver());
  return hdf5::file::create("dummyfile.h5", hdf5::file::AccessFlags::TRUNCATE,
                            hdf5::property::FileCreationList(), fapl);
}

TEST(H5, writeStringToDataset) {
  auto File = createInMemoryFile();
  hdf5::dataspace::Simple Space({0}, {H5S_UNLIMITED});
  hdf5::property::DatasetCreationList DCPL;
  auto Type = hdf5::datatype::String::variable();
  Type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  DCPL.chunk({1});
  auto ds = h5::h5d::create(File.root(), "DummyDataset", Type, Space, DCPL);
  ASSERT_NE(ds, nullptr);
  std::string ExpectedValue("Some string value");
  ds->append(ExpectedValue);

  // Read back
  auto Dataset = File.root().get_dataset("DummyDataset");
  hdf5::dataspace::Simple SpaceFile({1});
  hdf5::dataspace::Simple SpaceMem({1});
  SpaceFile.selection.all();
  SpaceMem.selection.all();
  std::vector<char const *> Data;
  Data.resize(1);
  // Dataset.read(Data, Type, SpaceMem, SpaceFile);
  H5Dread(static_cast<hid_t>(Dataset), static_cast<hid_t>(Type),
          static_cast<hid_t>(SpaceMem), static_cast<hid_t>(SpaceFile),
          H5P_DEFAULT, Data.data());
  ASSERT_EQ(ExpectedValue, std::string(Data.at(0)));
}

TEST(H5, writeUsingChunked1DString) {
  auto File = createInMemoryFile();
  hdf5::dataspace::Simple Space({0}, {H5S_UNLIMITED});
  hdf5::property::DatasetCreationList DCPL;
  auto Type = hdf5::datatype::String::variable();
  Type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  DCPL.chunk({1});
  std::string ExpectedValue("Some string value");
  auto ChunkedDataset =
      h5::Chunked1DString::create(File.root(), "DummyDataset", 64);
  ChunkedDataset->append(ExpectedValue);

  // Read back
  auto Dataset = File.root().get_dataset("DummyDataset");
  hdf5::dataspace::Simple SpaceFile({1});
  hdf5::dataspace::Simple SpaceMem({1});
  SpaceFile.selection.all();
  SpaceMem.selection.all();
  std::vector<char const *> Data;
  Data.resize(1);
  // Dataset.read(Data, Type, SpaceMem, SpaceFile);
  H5Dread(static_cast<hid_t>(Dataset), static_cast<hid_t>(Type),
          static_cast<hid_t>(SpaceMem), static_cast<hid_t>(SpaceFile),
          H5P_DEFAULT, Data.data());
  ASSERT_EQ(ExpectedValue, std::string(Data.at(0)));
}

TEST(H5, writeScalarString) {
  auto File = createInMemoryFile();
  auto Group = File.root();
  std::string SourceName("value");

  using FileWriter::Schemas::f142::Mode;
  FileWriter::Schemas::f142::WriterScalarString Writer(Group, SourceName,
                                                       Mode::Create);
  ASSERT_TRUE(Group.get_dataset("value").datatype() ==
              hdf5::datatype::String::variable().native_type());
}
