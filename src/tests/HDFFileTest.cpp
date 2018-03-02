#include "../HDFFile.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>

FileWriter::HDFFile createInMemoryTestFile(const std::string &Filename) {
  hdf5::property::FileAccessList fapl;
  fapl.driver(hdf5::file::MemoryDriver());

  FileWriter::HDFFile TestFile;
  TestFile.h5file =
      hdf5::file::create(Filename, hdf5::file::AccessFlags::TRUNCATE,
                         hdf5::property::FileCreationList(), fapl);

  return TestFile;
}

TEST(HDFFileTest, whenCommandContainsScalarStringAttributeItIsWrittenToFile) {
  using namespace hdf5;

  auto TestFile = createInMemoryTestFile("test-scalar-string-attribute.nxs");

  std::string CommandWithScalarStringAttr = R""({
      "nexus_structure": {
        "children": [
          {
            "type": "group",
            "name": "group_with_scalar_string_attr",
            "attributes": {
              "hello": "world"
            }
          }
        ]
      }
    })"";
  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithScalarStringAttr, EmptyStreamHDFInfo);

  auto StringAttr = hdf5::node::get_group(TestFile.root_group,
                                          "/group_with_scalar_string_attr")
                        .attributes["hello"];
  ASSERT_EQ(StringAttr.datatype().get_class(), hdf5::datatype::Class::STRING);
  std::string StringValue;
  StringAttr.read(StringValue, StringAttr.datatype());
  ASSERT_EQ(StringValue, "world");
}

TEST(HDFFileTest, whenCommandContainsArrayOfAttributesTheyAreWrittenToFile) {
  using namespace hdf5;

  auto TestFile = createInMemoryTestFile("test-array-of-attributes.nxs");

  std::string CommandWithArrayOfAttrs = R""({
    "nexus_structure": {
      "children": [
        {
          "type": "group",
          "name": "group_with_array_of_attrs",
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

  auto IntAttr =
      node::get_group(TestFile.root_group, "group_with_array_of_attrs")
          .attributes["integer_attribute"];
  int64_t IntValue;
  IntAttr.read(IntValue);
  ASSERT_EQ(IntValue, 42);

  auto StringAttr =
      node::get_group(TestFile.root_group, "group_with_array_of_attrs")
          .attributes["string_attribute"];
  std::string StringValue;
  StringAttr.read(StringValue, StringAttr.datatype());
  ASSERT_EQ(StringValue, "string_value");
}

TEST(HDFFileTest, whenCommandContainsAttrOfSpecifiedTypeItIsWrittenToFile) {
  using namespace hdf5;

  auto TestFile = createInMemoryTestFile("test-typed-attribute.nxs");

  std::string CommandWithTypedAttrs = R""({
    "nexus_structure": {
      "children": [
        {
          "type": "group",
          "name": "group_with_typed_attrs",
          "attributes": [
            {
              "name": "uint32_attribute",
              "values": 42,
              "dataset": {
                "type": "uint32"
              }
            }
          ]
        }
      ]
    }
  })"";

  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithTypedAttrs, EmptyStreamHDFInfo);

  auto IntAttr =
      node::get_group(TestFile.root_group, "group_with_typed_attrs")
          .attributes["uint32_attribute"];
  uint32_t IntValue;
  IntAttr.read(IntValue);
  ASSERT_EQ(IntValue, 42);
}

TEST(HDFFileTest, whenCommandContainsArrayAttrItIsWrittenToFile) {
  using namespace hdf5;

  auto TestFile = createInMemoryTestFile("test-array-attribute.nxs");

  std::string CommandWithArrayAttr = R""({
    "nexus_structure": {
      "children": [
        {
          "type": "group",
          "name": "group_with_array_attr",
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

  auto ArrayAttr =
      node::get_group(TestFile.root_group, "group_with_vector_attr")
          .attributes["array"];
  std::vector<int> ArrayAttrValues;
  ArrayAttr.read(ArrayAttrValues);
  ASSERT_EQ(ArrayAttrValues[0], 1);
  ASSERT_EQ(ArrayAttrValues[1], 2);
  ASSERT_EQ(ArrayAttrValues[2], 3);
}
