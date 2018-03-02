#include "../HDFFile.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>

FileWriter::HDFFile createInMemoryTestFile(const std::string &Filename) {
  hdf5::property::FileAccessList fapl;
  fapl.driver(hdf5::file::MemoryDriver());

  FileWriter::HDFFile TestFile;
  TestFile.h5file = hdf5::file::create(
          Filename, hdf5::file::AccessFlags::TRUNCATE,
          hdf5::property::FileCreationList(), fapl);

  return TestFile;
}

TEST(HDFFileTest, whenCommandContainsArrayOfAttributesTheyAreWrittenToFile) {
  using namespace hdf5;

  auto TestFile = createInMemoryTestFile("test-array-of-attributes.nxs");

  std::string CommandWithArrayOfAttrs = R""({
    "nexus_structure": {
      "children": [
        {
          "type": "dataset",
          "name": "dataset_with_array_of_attrs",
          "values": 42.24,
          "attributes": [
            {
              "name": "integer_attribute",
              "values": 42
            },
            {
              "name": "string_attribute",
              "values": "string_value"
            }
          ]
        }
      ]
    }
  })"";

  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithArrayOfAttrs, EmptyStreamHDFInfo);

  auto dataset =
      node::get_dataset(TestFile.root_group, "dataset_with_array_of_attrs");

  ASSERT_TRUE(dataset.attributes.exists("integer_attribute"));
  auto int_attr = dataset.attributes["integer_attribute"];
  uint64_t int_attr_value;
  int_attr.read(int_attr_value);
  ASSERT_EQ(int_attr_value, 42);

  ASSERT_TRUE(dataset.attributes.exists("string_attribute"));
  auto string_attr = dataset.attributes["string_attribute"];
  std::string str_attr_value;
  int_attr.read(str_attr_value);
  ASSERT_EQ(str_attr_value, "string_value");
}

TEST(HDFFileTest, whenCommandContainsArrayAttrItIsWrittenToFile) {
  using namespace hdf5;

  auto TestFile = createInMemoryTestFile("test-array-attribute.nxs");

  std::string CommandWithArrayAttr = R""({
    "nexus_structure": {
      "children": [
        {
          "type": "dataset",
          "name": "dataset_with_array_attr",
          "values": 42.24,
          "attributes": [
            {
              "name": "array",
              "values":[1, 2, 3],
              "dataset": {
                "type": "uint64"
              }
            }
          ]
        }
      ]
    }
  })"";

  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithArrayAttr, EmptyStreamHDFInfo);

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
