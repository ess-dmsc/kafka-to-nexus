// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDFOperations.h"
#include "helpers/HDFFileTestHelper.h"
#include <gtest/gtest.h>

TEST(JSonArrayDimensions, Array1) {
  auto JSonObj = nlohmann::json::parse("[1]");
  auto FoundDimensions = HDFOperations::determineArrayDimensions(JSonObj);
  EXPECT_EQ(FoundDimensions, Shape{1});
}

TEST(JSonArrayDimensions, Array2) {
  auto JSonObj = nlohmann::json::parse("[1, 2, 3]");
  auto FoundDimensions = HDFOperations::determineArrayDimensions(JSonObj);
  EXPECT_EQ(FoundDimensions, Shape{3});
}

TEST(JSonArrayDimensions, Array3) {
  auto JSonObj = nlohmann::json::parse("[[1, 2, 3], [1, 2, 3]]");
  auto FoundDimensions = HDFOperations::determineArrayDimensions(JSonObj);
  EXPECT_EQ(FoundDimensions, Shape({2, 3}));
}

TEST(JSonArrayDimensions, Array4) {
  auto JSonObj = nlohmann::json::parse("[[3], [1, 2, 3]]");
  auto FoundDimensions = HDFOperations::determineArrayDimensions(JSonObj);
  EXPECT_EQ(FoundDimensions, Shape({2, 3}));
}

TEST(JSonArrayDimensions, Array5) {
  auto JSonObj = nlohmann::json::parse("[[], [[1,2,3], [1,2,3]]]");
  auto FoundDimensions = HDFOperations::determineArrayDimensions(JSonObj);
  EXPECT_EQ(FoundDimensions, Shape({2, 2, 3}));
}

TEST(JsonArrayToMultiVector, Array1) {
  auto JsonObj = nlohmann::json::parse("[1]");
  std::vector<int> Data{1};
  MultiVector<int> ExpectedResult({1});
  std::copy(Data.begin(), Data.end(), ExpectedResult.Data.begin());
  EXPECT_EQ(ExpectedResult, HDFOperations::jsonArrayToMultiArray<int>(JsonObj));
}

TEST(JsonArrayToMultiVector, Array2) {
  auto JsonObj = nlohmann::json::parse("[1, 2, 4]");
  std::vector<int> Data{1, 2, 4};
  MultiVector<int> ExpectedResult({3});
  std::copy(Data.begin(), Data.end(), ExpectedResult.Data.begin());
  EXPECT_EQ(ExpectedResult, HDFOperations::jsonArrayToMultiArray<int>(JsonObj));
}

TEST(JsonArrayToMultiVector, Array3) {
  auto JsonObj = nlohmann::json::parse("[[1, 2, 4], [1, 2, 4]]");
  std::vector<int> Data{1, 1, 2, 2, 4, 4};
  MultiVector<int> ExpectedResult({2, 3});
  std::copy(Data.begin(), Data.end(), ExpectedResult.Data.begin());
  EXPECT_EQ(ExpectedResult, HDFOperations::jsonArrayToMultiArray<int>(JsonObj));
}

TEST(JsonArrayToMultiVector, Array4) {
  auto JsonObj = nlohmann::json::parse(
      "[[[1, 2, 4], [1, 2, 4]], [[3, 6, 8], [10, 20, 40]]]");
  std::vector<int> Data{1, 3, 1, 10, 2, 6, 2, 20, 4, 8, 4, 40};
  MultiVector<int> ExpectedResult({2, 2, 3});
  std::copy(Data.begin(), Data.end(), ExpectedResult.Data.begin());
  EXPECT_EQ(ExpectedResult, HDFOperations::jsonArrayToMultiArray<int>(JsonObj));
}

TEST(JsonArrayToMultiVector, Array5) {
  auto JsonObj = nlohmann::json::parse("42");
  std::vector<int> Data{42};
  MultiVector<int> ExpectedResult({1});
  std::copy(Data.begin(), Data.end(), ExpectedResult.Data.begin());
  EXPECT_EQ(ExpectedResult, HDFOperations::jsonArrayToMultiArray<int>(JsonObj));
}

class HDFStaticDataTest : public ::testing::Test {
public:
  void SetUp() override {
    TestFile =
        HDFFileTestHelper::createInMemoryTestFile("test-static-data.nxs", true);
    RootGroup = TestFile->hdfGroup();
  }
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> TestFile;
  hdf5::node::Group RootGroup;
};

TEST_F(HDFStaticDataTest, UntypedSingleInt) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "values" : 3
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  HDFOperations::writeDataset(RootGroup, Temp);
  auto HDFDataset = hdf5::node::get_dataset(TestFile->hdfGroup(), "/some_name");
  std::vector<double> DatasetValues(1);
  HDFDataset.read(DatasetValues);
  std::vector<double> ExpectedDataset{3};
  EXPECT_EQ(DatasetValues, ExpectedDataset);
}

TEST_F(HDFStaticDataTest, UntypedSingleIntAlt) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "values" : [3]
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  HDFOperations::writeDataset(RootGroup, Temp);
  auto HDFDataset = hdf5::node::get_dataset(TestFile->hdfGroup(), "/some_name");
  std::vector<double> DatasetValues(1);
  HDFDataset.read(DatasetValues);
  std::vector<double> ExpectedDataset{3};
  EXPECT_EQ(DatasetValues, ExpectedDataset);
}

TEST_F(HDFStaticDataTest, UntypedSingleFloat) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "values" : [3.145]
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  HDFOperations::writeDataset(RootGroup, Temp);
  auto HDFDataset = hdf5::node::get_dataset(TestFile->hdfGroup(), "/some_name");
  std::vector<double> DatasetValues(1);
  HDFDataset.read(DatasetValues);
  std::vector<double> ExpectedDataset{3.145};
  EXPECT_EQ(DatasetValues, ExpectedDataset);
}

TEST_F(HDFStaticDataTest, UntypedSingleFloatAlt) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "values" : 3.145
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  HDFOperations::writeDataset(RootGroup, Temp);
  auto HDFDataset = hdf5::node::get_dataset(TestFile->hdfGroup(), "/some_name");
  std::vector<double> DatasetValues(1);
  HDFDataset.read(DatasetValues);
  std::vector<double> ExpectedDataset{3.145};
  EXPECT_EQ(DatasetValues, ExpectedDataset);
}

TEST_F(HDFStaticDataTest, UntypedSingleString) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "values" : "Hello"
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  EXPECT_THROW(HDFOperations::writeDataset(RootGroup, Temp),
               std::runtime_error);
}

TEST_F(HDFStaticDataTest, UntypedSingleStringAlt) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "values" : ["Hello"]
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  EXPECT_THROW(HDFOperations::writeDataset(RootGroup, Temp),
               std::runtime_error);
}

TEST_F(HDFStaticDataTest, IntArray1) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "dtype": "uint64",
    "values" : [1, 2, 3]
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  HDFOperations::writeDataset(RootGroup, Temp);
  auto HDFDataset = hdf5::node::get_dataset(TestFile->hdfGroup(), "/some_name");
  std::vector<uint64_t> DatasetValues(3);
  HDFDataset.read(DatasetValues);
  std::vector<uint64_t> ExpectedDataset{1, 2, 3};
  EXPECT_EQ(DatasetValues, ExpectedDataset);
}

TEST_F(HDFStaticDataTest, IntArray2) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "dtype": "uint64",
    "values" : [[1, 2, 3], [4, 5, 6]]
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  HDFOperations::writeDataset(RootGroup, Temp);
  auto HDFDataset = hdf5::node::get_dataset(TestFile->hdfGroup(), "/some_name");
  std::vector<uint64_t> DatasetValues(6);
  HDFDataset.read(DatasetValues);
  std::vector<uint64_t> ExpectedDataset{1, 4, 2, 5, 3, 6};
  EXPECT_EQ(DatasetValues, ExpectedDataset);
  hdf5::Dimensions ExpectedDimensions{2, 3};

  auto DataSpace = HDFDataset.dataspace();
  hdf5::dataspace::Simple SomeSpace(DataSpace);
  auto CDims = SomeSpace.current_dimensions();

  EXPECT_EQ(CDims, ExpectedDimensions);
  EXPECT_EQ(HDFDataset.dataspace().size(), 6);
}

TEST_F(HDFStaticDataTest, SingleString) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "dtype": "string",
    "values" : "some string"
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  HDFOperations::writeDataset(RootGroup, Temp);
  auto HDFDataset = hdf5::node::get_dataset(TestFile->hdfGroup(), "/some_name");
  MultiVector<std::string> DatasetValues({1});
  HDFDataset.read(DatasetValues.Data);
  MultiVector<std::string> ExpectedDataset({1});
  ExpectedDataset.Data[0] = "some string";
  EXPECT_EQ(DatasetValues, ExpectedDataset);
}

TEST_F(HDFStaticDataTest, SingleStringAlt) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "dtype": "string",
    "values" : ["some string"]
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  HDFOperations::writeDataset(RootGroup, Temp);
  auto HDFDataset = hdf5::node::get_dataset(TestFile->hdfGroup(), "/some_name");
  MultiVector<std::string> DatasetValues({1});
  HDFDataset.read(DatasetValues.Data);
  MultiVector<std::string> ExpectedDataset({1});
  ExpectedDataset.Data[0] = "some string";
  EXPECT_EQ(DatasetValues, ExpectedDataset);
}

TEST_F(HDFStaticDataTest, StringArray) {
  std::string JsonString = R""(
  {
    "name": "some_name",
    "dtype": "string",
    "values" : [["a", "b"], ["c", "d"]]
  })"";
  auto Temp = nlohmann::json::parse(JsonString);
  HDFOperations::writeDataset(RootGroup, Temp);
  auto HDFDataset = hdf5::node::get_dataset(TestFile->hdfGroup(), "/some_name");
  MultiVector<std::string> DatasetValues({2, 2});
  HDFDataset.read(DatasetValues.Data);
  MultiVector<std::string> ExpectedDataset({2, 2});
  ExpectedDataset.at({0, 0}) = "a";
  ExpectedDataset.at({0, 1}) = "b";
  ExpectedDataset.at({1, 0}) = "c";
  ExpectedDataset.at({1, 1}) = "d";
  EXPECT_EQ(DatasetValues, ExpectedDataset);
}

// TEST_F(HDFStaticDataTest, AddLinkToNode1) {
//  RootGroup.create_group("data_to_link");
//  RootGroup.create_group("data_link");
//  std::string JsonString = R""({
//    "name": "data_link",
//    "type": "link",
//    "target": "/data_to_link"})"";
//  auto Temp = nlohmann::json::parse(JsonString);
//  HDFOperations::addLinkToNode(RootGroup, Temp);
//  auto link = RootGroup.links["data_link"];
//  ASSERT_TRUE(link.is_resolvable());
//  ASSERT_TRUE(link.type() == hdf5::node::LinkType::HARD);
//}
//
// TEST_F(HDFStaticDataTest, AddLinkToNode2) {
//  RootGroup.create_group("data_link");
//  std::string JsonString = R""({
//    "name": "data_link",
//    "type": "link",
//    "target": "/data/data_to_link"})"";
//  auto Temp = nlohmann::json::parse(JsonString);
//  HDFOperations::addLinkToNode(RootGroup, Temp);
//  auto link = RootGroup.links["data_link"];
//  ASSERT_FALSE(link.is_resolvable());
//}
