#include "../HDFFile.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>

FileWriter::HDFFile createInMemoryTestFile(const std::string &Filename) {
  auto fapl = H5Pcreate(H5P_FILE_ACCESS);
  // _core uses the in-memory file driver
  H5Pset_fapl_core(fapl, 100 * 1024, H5P_DEFAULT);

  auto file_id = H5Fcreate(Filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
  H5Pclose(fapl);

  FileWriter::HDFFile TestFile;
  TestFile.h5file = hdf5::file::File(hdf5::ObjectHandle(file_id));

  return TestFile;
}

TEST(HDFFileTest, givenCommandContainsVectorAttrItIsWrittenToFile) {
  using namespace hdf5;

  auto TestFile = createInMemoryTestFile("test-vector-attribute.nxs");

  std::string CommandWithVectorAttr = R""({
    "nexus_structure": {
      "children": [
        {
          "type": "dataset",
          "name": "dataset_with_vector_attr",
          "values": 42.24,
          "attributes": {"vector":[1, 2, 3]}
        }
      ]
    }
  })"";

  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithVectorAttr, EmptyStreamHDFInfo);

  auto dataset =
      node::get_dataset(TestFile.root_group, "dataset_with_vector_attr");

  ASSERT_TRUE(dataset.attributes.exists("vector"));
  auto vector_attr = dataset.attributes["vector"];
  std::vector<int> vector_attr_values;
  vector_attr.read(vector_attr_values);
  ASSERT_EQ(vector_attr_values[0], 1);
  ASSERT_EQ(vector_attr_values[1], 2);
  ASSERT_EQ(vector_attr_values[2], 3);
}
