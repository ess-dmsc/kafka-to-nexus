// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDF5/HDFFile.h"
#include "helpers/HDFFileTestHelper.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>

class HDFFileAttributesTest : public ::testing::Test {
public:
  void SetUp() override {
    TestFile =
        HDFFileTestHelper::createInMemoryTestFile("test-attribute.nxs", true);
  }
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> TestFile;
};

TEST_F(HDFFileAttributesTest,
       whenCommandContainsNumericalAttributeItIsWrittenToFile) {
  std::string CommandWithNumericalAttr = R""({
      "children": [
        {
          "module": "dataset",
          "config": {
            "name": "dataset_with_numerical_attr",
            "values" : 3
          },
          "attributes": {
            "the_answer_is": 42
          }
        }
      ]
    })"";
  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithNumericalAttr, EmptyModuleHDFInfo);

  auto Attr = hdf5::node::get_dataset(TestFile->hdfGroup(),
                                      "/dataset_with_numerical_attr")
                  .attributes["the_answer_is"];
  int AttrValue{0};
  Attr.read(AttrValue);
  ASSERT_EQ(AttrValue, 42);
}

TEST_F(HDFFileAttributesTest,
       whenCommandContainsScalarStringAttributeItIsWrittenToFile) {
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
  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithScalarStringAttr, EmptyModuleHDFInfo);

  auto StringAttr = hdf5::node::get_group(TestFile->hdfGroup(),
                                          "/group_with_scalar_string_attr")
                        .attributes["hello"];
  ASSERT_EQ(StringAttr.datatype().get_class(), hdf5::datatype::Class::String);
  std::string StringValue;
  StringAttr.read(StringValue, StringAttr.datatype());
  ASSERT_EQ(StringValue, "world");
}

TEST_F(HDFFileAttributesTest,
       whenCommandContainsArrayOfAttributesTheyAreWrittenToFile) {
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

  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithArrayOfAttrs, EmptyModuleHDFInfo);

  auto IntAttr =
      hdf5::node::get_group(TestFile->hdfGroup(), "group_with_array_of_attrs")
          .attributes["integer_attribute"];
  int64_t IntValue{0};
  IntAttr.read(IntValue);
  ASSERT_EQ(IntValue, 42);

  auto StringAttr =
      hdf5::node::get_group(TestFile->hdfGroup(), "group_with_array_of_attrs")
          .attributes["string_attribute"];
  std::string StringValue;
  StringAttr.read(StringValue, StringAttr.datatype());
  ASSERT_EQ(StringValue, "string_value");
}

TEST_F(HDFFileAttributesTest,
       whenCommandContainsAttrOfSpecifiedTypeItIsWrittenToFile) {
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

  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithTypedAttrs, EmptyModuleHDFInfo);

  auto IntAttr =
      hdf5::node::get_group(TestFile->hdfGroup(), "group_with_typed_attrs")
          .attributes["uint32_attribute"];
  uint32_t IntValue{0};
  IntAttr.read(IntValue);
  ASSERT_EQ(IntValue, 42u);
}

TEST_F(HDFFileAttributesTest, IntArrayAttribute) {
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
          }
        ]
      }
    ]
  })"";

  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithArrayAttr, EmptyModuleHDFInfo);

  auto ArrayAttr =
      hdf5::node::get_group(TestFile->hdfGroup(), "group_with_array_attrs")
          .attributes["array_attribute"];
  std::vector<int> ArrayAttrValues(3);
  ArrayAttr.read(ArrayAttrValues);
  ASSERT_EQ(ArrayAttrValues[0], 1);
  ASSERT_EQ(ArrayAttrValues[1], 2);
  ASSERT_EQ(ArrayAttrValues[2], 3);
}

TEST_F(HDFFileAttributesTest, StringArrayAttribute) {
  std::string CommandWithArrayAttr = R""({
    "children": [
      {
        "type": "group",
        "name": "group_with_array_attrs",
        "attributes": [
          {
            "name": "array_string_attribute",
            "values": ["A", "B"],
            "type": "string"
          }
        ]
      }
    ]
  })"";

  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithArrayAttr, EmptyModuleHDFInfo);

  auto ArrayStringAttr =
      hdf5::node::get_group(TestFile->hdfGroup(), "group_with_array_attrs")
          .attributes["array_string_attribute"];
  std::vector<std::string> ArrayStringAttrValues(2);
  ArrayStringAttr.read(ArrayStringAttrValues);
  ASSERT_EQ(ArrayStringAttrValues[0], "A");
  ASSERT_EQ(ArrayStringAttrValues[1], "B");
}

TEST_F(HDFFileAttributesTest,
       ArrayOfAttributesWithFixedLengthStringItIsWrittenAsFixedLengthStrings) {
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
          },
          {
            "name": "string_variable_ascii_attribute",
            "values": "string_value",
            "type": "string"
          },
          {
            "name": "string_variable_ascii_array_attribute",
            "values": ["string_value_0", "string_value_1", "string_value_2"],
            "type": "string"
          },
          {
            "name": "string_fixed_ascii_attribute",
            "values": "string_value",
            "type": "string",
            "string_size": 32
          }
        ]
      }
    ]
  })"";

  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithArrayOfAttrs, EmptyModuleHDFInfo);

  {
    auto StringAttr =
        hdf5::node::get_group(TestFile->hdfGroup(), "group_with_attributes")
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
        hdf5::node::get_group(TestFile->hdfGroup(), "group_with_attributes")
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
        hdf5::node::get_group(TestFile->hdfGroup(), "group_with_attributes")
            .attributes["string_fixed_attribute"];
    auto Type = hdf5::datatype::String(StringAttr.datatype());
    EXPECT_EQ(Type.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    std::string StringValue;
    StringAttr.read(StringValue, StringAttr.datatype());
    std::string Expected("string_value");
    StringValue.resize(Expected.size());
    EXPECT_EQ(StringValue, Expected.data());
  }

  {
    auto StringArrayAttr =
        hdf5::node::get_group(TestFile->hdfGroup(), "group_with_attributes")
            .attributes["string_fixed_array_attribute"];
    auto Type = hdf5::datatype::String(StringArrayAttr.datatype());
    ASSERT_EQ(Type.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    std::vector<std::string> Buffer(3);
    StringArrayAttr.read(Buffer);
    std::vector<std::string> Expected{"string_value_0", "string_value_1",
                                      "string_value_2"};
    ASSERT_EQ(Buffer, Expected);
  }

  {
    auto StringAttr =
        hdf5::node::get_group(TestFile->hdfGroup(), "group_with_attributes")
            .attributes["string_variable_ascii_attribute"];
    auto Type = hdf5::datatype::String(StringAttr.datatype());
    ASSERT_TRUE(Type.is_variable_length());
    std::string StringValue;
    StringAttr.read(StringValue, StringAttr.datatype());
    ASSERT_EQ(StringValue, "string_value");
  }

  {
    auto StringArrayAttr =
        hdf5::node::get_group(TestFile->hdfGroup(), "group_with_attributes")
            .attributes["string_variable_ascii_array_attribute"];
    auto Type = hdf5::datatype::String(StringArrayAttr.datatype());
    ASSERT_TRUE(Type.is_variable_length());
    std::vector<std::string> Buffer;
    Buffer.resize(3);
    StringArrayAttr.read(Buffer, StringArrayAttr.datatype());
  }

  {
    auto StringAttr =
        hdf5::node::get_group(TestFile->hdfGroup(), "group_with_attributes")
            .attributes["string_fixed_ascii_attribute"];
    std::string StringValue;
    StringAttr.read(StringValue, StringAttr.datatype());
    std::string Expected("string_value");
    StringValue.resize(Expected.size());
    ASSERT_EQ(StringValue, Expected.data());
  }
}

TEST_F(HDFFileAttributesTest, ObjectOfAttributesOfTypeString) {
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

  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(Command, EmptyModuleHDFInfo);

  {
    auto StringAttr = hdf5::node::get_group(TestFile->hdfGroup(),
                                            "group_with_object_of_attributes")
                          .attributes["some_attribute"];
    auto Type = hdf5::datatype::String(StringAttr.datatype());
    ASSERT_TRUE(Type.is_variable_length());
    ASSERT_EQ(Type.encoding(), hdf5::datatype::CharacterEncoding::UTF8);
    std::string StringValue;
    StringAttr.read(StringValue, StringAttr.datatype());
    ASSERT_EQ(StringValue, "Some Value");
  }
}

TEST_F(HDFFileAttributesTest, NumArrayAttributeWithoutType) {
  std::string CommandWithNumericalAttr = R""({
      "children": [
        {
          "module": "dataset",
          "config": {
            "name": "dataset_with_numerical_attr",
            "values" : 3
          },
          "attributes": [
            {
              "name": "vec",
              "values": [1,-2,4.234]
            }
          ]
        }
      ]
    })"";
  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithNumericalAttr, EmptyModuleHDFInfo);

  auto Attr = hdf5::node::get_dataset(TestFile->hdfGroup(),
                                      "/dataset_with_numerical_attr")
                  .attributes["vec"];
  std::vector<double> AttrValue(3);
  Attr.read(AttrValue);
  std::vector<double> ExpectedAttr{1, -2, 4.234};
  EXPECT_EQ(AttrValue, ExpectedAttr);
}

TEST_F(HDFFileAttributesTest, StringArrayAttributeWithoutType) {
  std::string CommandWithNumericalAttr = R""({
      "children": [
        {
          "module": "dataset",
          "config": {
            "name": "dataset_with_numerical_attr",
            "values" : 3
          },
          "attributes": [
            {
              "name": "vec",
              "values": ["one", "two", "three", "four"]
            }
          ]
        }
      ]
    })"";
  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithNumericalAttr, EmptyModuleHDFInfo);

  auto Attr = hdf5::node::get_dataset(TestFile->hdfGroup(),
                                      "/dataset_with_numerical_attr")
                  .attributes["vec"];
  std::vector<std::string> AttrValue(4);
  Attr.read(AttrValue);
  std::vector<std::string> ExpectedAttr{"one", "two", "three", "four"};
  EXPECT_EQ(AttrValue, ExpectedAttr);
}

TEST_F(HDFFileAttributesTest, MixedArrayAttributeWithoutType) {
  std::string CommandWithNumericalAttr = R""({
      "children": [
        {
          "module": "dataset",
          "config": {
            "name": "dataset_with_numerical_attr",
            "values" : 3
          },
          "attributes": [
            {
              "name": "vec",
              "values": ["one", 2, "three", "four"]
            }
          ]
        }
      ]
    })"";
  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithNumericalAttr, EmptyModuleHDFInfo);

  auto Attr = hdf5::node::get_dataset(TestFile->hdfGroup(),
                                      "/dataset_with_numerical_attr")
                  .attributes["vec"];
  std::vector<std::string> AttrValue(4);
  Attr.read(AttrValue);
  std::vector<std::string> ExpectedAttr{"one", "2", "three", "four"};
  EXPECT_EQ(AttrValue, ExpectedAttr);
}

TEST_F(HDFFileAttributesTest, EmptyStringArrayAttributeWithoutType) {
  std::string CommandWithNumericalAttr = R""({
      "children": [
        {
          "module": "dataset",
          "config": {
            "name": "dataset_with_numerical_attr",
            "values" : 3
          },
          "attributes": [
            {
              "name": "vec",
              "values": ["", ""]
            }
          ]
        }
      ]
    })"";
  std::vector<ModuleHDFInfo> EmptyModuleHDFInfo;
  TestFile->init(CommandWithNumericalAttr, EmptyModuleHDFInfo);

  auto Attr = hdf5::node::get_dataset(TestFile->hdfGroup(),
                                      "/dataset_with_numerical_attr")
                  .attributes["vec"];
  std::vector<std::string> AttrValue(2);
  Attr.read(AttrValue);
  std::vector<std::string> ExpectedAttr{"", ""};
  EXPECT_EQ(AttrValue, ExpectedAttr);
}

// Add empty string value test
