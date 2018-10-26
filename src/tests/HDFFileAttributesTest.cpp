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

  auto ArrayAttr = node::get_group(TestFile.RootGroup, "group_with_array_attrs")
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

TEST(HDFFileAttributesTest,
     ArrayOfAttributesWithFixedLengthStringItIsWrittenAsFixedLengthStrings) {
  using namespace hdf5;

  auto TestFile = HDFFileTestHelper::createInMemoryTestFile(
      "test-array-of-attributes-fixed-length.nxs");

  std::string CommandWithArrayOfAttrs = R""({
    "children": [
      {
        "type": "group",
        "name": "group_with_attributes",
        "attributes": [
          {
            "name": "string_variable_attribute",
            "values": "string_value",
            "type": "string"
          },
          {
            "name": "string_variable_array_attribute",
            "values": ["string_value_0", "string_value_1", "string_value_2"],
            "type": "string"
          },
          {
            "name": "string_fixed_attribute",
            "values": "string_value",
            "type": "string",
            "string_size": 32
          },
          {
            "name": "string_fixed_array_attribute",
            "values": ["string_value_0", "string_value_1", "string_value_2"],
            "type": "string",
            "string_size": 32
          }
        ]
      }
    ]
  })"";

  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(CommandWithArrayOfAttrs, EmptyStreamHDFInfo);

  {
    auto StringAttr =
        node::get_group(TestFile.RootGroup, "group_with_attributes")
            .attributes["string_variable_attribute"];
    auto Type = hdf5::datatype::String(StringAttr.datatype());
    ASSERT_TRUE(Type.is_variable_length());
    ASSERT_EQ(Type.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    std::string StringValue;
    StringAttr.read(StringValue, StringAttr.datatype());
    ASSERT_EQ(StringValue, "string_value");
  }

  {
    auto StringArrayAttr =
        node::get_group(TestFile.RootGroup, "group_with_attributes")
            .attributes["string_variable_array_attribute"];
    auto Type = hdf5::datatype::String(StringArrayAttr.datatype());
    ASSERT_TRUE(Type.is_variable_length());
    ASSERT_EQ(Type.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    std::vector<std::string> Buffer;
    Buffer.resize(3);
    StringArrayAttr.read(Buffer, StringArrayAttr.datatype());
  }

  {
    auto StringAttr =
        node::get_group(TestFile.RootGroup, "group_with_attributes")
            .attributes["string_fixed_attribute"];
    auto Type = hdf5::datatype::String(StringAttr.datatype());
    ASSERT_FALSE(Type.is_variable_length());
    ASSERT_EQ(Type.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    std::string StringValue;
    StringAttr.read(StringValue, StringAttr.datatype());
    std::string Expected("string_value");
    StringValue.resize(Expected.size());
    ASSERT_EQ(StringValue, Expected.data());
  }

  {
    auto StringArrayAttr =
        node::get_group(TestFile.RootGroup, "group_with_attributes")
            .attributes["string_fixed_array_attribute"];
    auto Type = hdf5::datatype::String(StringArrayAttr.datatype());
    ASSERT_FALSE(Type.is_variable_length());
    ASSERT_EQ(Type.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    std::vector<char> Buffer(3 * 32);
    ASSERT_LE(0, H5Aread(static_cast<hid_t>(StringArrayAttr),
                         static_cast<hid_t>(StringArrayAttr.datatype()),
                         Buffer.data()));
    ASSERT_EQ(std::string(Buffer.data() + 0 * 32), "string_value_0");
    ASSERT_EQ(std::string(Buffer.data() + 1 * 32), "string_value_1");
    ASSERT_EQ(std::string(Buffer.data() + 2 * 32), "string_value_2");
  }
}

TEST(HDFFileAttributesTest, ObjectOfAttributesOfTypeString) {
  using namespace hdf5;

  auto TestFile = HDFFileTestHelper::createInMemoryTestFile(
      "test-object-of-attributes-with-strings.nxs");

  std::string Command = R""({
    "children": [
      {
        "type": "group",
        "name": "group_with_object_of_attributes",
        "attributes": {
          "some_attribute": "Some Value"
        }
      }
    ]
  })"";

  std::vector<FileWriter::StreamHDFInfo> EmptyStreamHDFInfo;
  TestFile.init(Command, EmptyStreamHDFInfo);

  {
    auto StringAttr =
        node::get_group(TestFile.RootGroup, "group_with_object_of_attributes")
            .attributes["some_attribute"];
    auto Type = hdf5::datatype::String(StringAttr.datatype());
    ASSERT_TRUE(Type.is_variable_length());
    ASSERT_EQ(Type.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    std::string StringValue;
    StringAttr.read(StringValue, StringAttr.datatype());
    ASSERT_EQ(StringValue, "Some Value");
  }
}
