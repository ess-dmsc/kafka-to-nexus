#include "HDFFile.h"
#include "HDFFileTestHelper.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>

TEST(HDFFileAttributesTest,
     whenCommandContainsNumericalAttributeItIsWrittenToFile) {
  using namespace hdf5;

  auto TestFile =
      HDFFileTestHelper::createInMemoryTestFile("test-numerical-attribute.nxs");

  std::string CommandWithNumericalAttr = R""({
      "children": [
        {
          "type": "dataset",
          "name": "dataset_with_numerical_attr",
          "values" : 3,
          "attributes": {
            "the_answer_is": 42
          }
        }
      ]
    })"";
  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithNumericalAttr, EmptyStreamHDFInfo);

  auto Attr = hdf5::node::get_dataset(TestFile.RootGroup,
                                      "/dataset_with_numerical_attr")
                  .attributes["the_answer_is"];
  int AttrValue;
  Attr.read(AttrValue);
  ASSERT_EQ(AttrValue, 42);
}

TEST(HDFFileAttributesTest,
     whenCommandContainsScalarStringAttributeItIsWrittenToFile) {
  using namespace hdf5;

  auto TestFile = HDFFileTestHelper::createInMemoryTestFile(
      "test-scalar-string-attribute.nxs");

  std::string CommandWithScalarStringAttr = R""({
      "children": [
        {
          "type": "group",
          "name": "group_with_scalar_string_attr",
          "attributes": {
            "hello": "world"
          }
        }
      ]
    })"";
  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithScalarStringAttr, EmptyStreamHDFInfo);

  auto StringAttr = hdf5::node::get_group(TestFile.RootGroup,
                                          "/group_with_scalar_string_attr")
                        .attributes["hello"];
  ASSERT_EQ(StringAttr.datatype().get_class(), hdf5::datatype::Class::STRING);
  std::string StringValue;
  StringAttr.read(StringValue, StringAttr.datatype());
  ASSERT_EQ(StringValue, "world");
}

TEST(HDFFileAttributesTest,
     whenCommandContainsArrayOfAttributesTheyAreWrittenToFile) {
  using namespace hdf5;

  auto TestFile =
      HDFFileTestHelper::createInMemoryTestFile("test-array-of-attributes.nxs");

  std::string CommandWithArrayOfAttrs = R""({
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
  })"";

  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithArrayOfAttrs, EmptyStreamHDFInfo);

  auto IntAttr =
      node::get_group(TestFile.RootGroup, "group_with_array_of_attrs")
          .attributes["integer_attribute"];
  int64_t IntValue;
  IntAttr.read(IntValue);
  ASSERT_EQ(IntValue, 42);

  auto StringAttr =
      node::get_group(TestFile.RootGroup, "group_with_array_of_attrs")
          .attributes["string_attribute"];
  std::string StringValue;
  StringAttr.read(StringValue, StringAttr.datatype());
  ASSERT_EQ(StringValue, "string_value");
}

TEST(HDFFileAttributesTest,
     whenCommandContainsAttrOfSpecifiedTypeItIsWrittenToFile) {
  using namespace hdf5;

  auto TestFile =
      HDFFileTestHelper::createInMemoryTestFile("test-typed-attribute.nxs");

  std::string CommandWithTypedAttrs = R""({
    "children": [
      {
        "type": "group",
        "name": "group_with_typed_attrs",
        "attributes": [
          {
            "name": "uint32_attribute",
            "values": 42,
            "type": "uint32"
          }
        ]
      }
    ]
  })"";

  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithTypedAttrs, EmptyStreamHDFInfo);

  auto IntAttr = node::get_group(TestFile.RootGroup, "group_with_typed_attrs")
                     .attributes["uint32_attribute"];
  uint32_t IntValue;
  IntAttr.read(IntValue);
  ASSERT_EQ(IntValue, 42u);
}

TEST(HDFFileAttributesTest, whenCommandContainsArrayAttrItIsWrittenToFile) {
  using namespace hdf5;

  auto TestFile =
      HDFFileTestHelper::createInMemoryTestFile("test-array-attribute.nxs");

  std::string CommandWithArrayAttr = R""({
    "children": [
      {
        "type": "group",
        "name": "group_with_array_attrs",
        "attributes": [
          {
            "name": "array_attribute",
            "values": [1, 2, 3],
            "type": "uint64"
          },
          {
            "name": "array_string_attribute",
            "values": ["A", "B"],
            "type": "string"
          }
        ]
      }
    ]
  })"";

  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithArrayAttr, EmptyStreamHDFInfo);

  auto ArrayAttr =
      node::get_group(TestFile.RootGroup, "group_with_array_attrs")
          .attributes["array_attribute"];
  std::vector<int> ArrayAttrValues(3);
  ArrayAttr.read(ArrayAttrValues);
  ASSERT_EQ(ArrayAttrValues[0], 1);
  ASSERT_EQ(ArrayAttrValues[1], 2);
  ASSERT_EQ(ArrayAttrValues[2], 3);

  auto ArrayStringAttr =
      node::get_group(TestFile.RootGroup, "group_with_array_attrs")
          .attributes["array_string_attribute"];
  std::vector<std::string> ArrayStringAttrValues(2);
  ArrayStringAttr.read(ArrayStringAttrValues);
  ASSERT_EQ(ArrayStringAttrValues[0], "A");
  ASSERT_EQ(ArrayStringAttrValues[1], "B");
}
