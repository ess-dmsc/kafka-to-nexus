#include "../HDFFile.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>

TEST(HDFFileTest, givenCommandContainsVectorAttrItIsWrittenToFile) {
  using namespace hdf5;

  FileWriter::HDFFile TestFile;
  TestFile.h5file =
      file::create("test-vector-attribute.h5", file::AccessFlags::TRUNCATE);

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
