// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDFOperations.h"
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
  std::copy(Data.begin(), Data.end(), ExpectedResult.begin());
  EXPECT_EQ(ExpectedResult, HDFOperations::jsonArrayToMultiArray<int>(JsonObj));
}

TEST(JsonArrayToMultiVector, Array2) {
  auto JsonObj = nlohmann::json::parse("[1, 2, 4]");
  std::vector<int> Data{1,2 , 4};
  MultiVector<int> ExpectedResult({3});
  std::copy(Data.begin(), Data.end(), ExpectedResult.begin());
  EXPECT_EQ(ExpectedResult, HDFOperations::jsonArrayToMultiArray<int>(JsonObj));
}

TEST(JsonArrayToMultiVector, Array3) {
  auto JsonObj = nlohmann::json::parse("[[1, 2, 4], [1, 2, 4]]");
  std::vector<int> Data{1, 1 , 2, 2 , 4, 4};
  MultiVector<int> ExpectedResult({2, 3});
  std::copy(Data.begin(), Data.end(), ExpectedResult.begin());
  EXPECT_EQ(ExpectedResult, HDFOperations::jsonArrayToMultiArray<int>(JsonObj));
}

TEST(JsonArrayToMultiVector, Array4) {
  auto JsonObj = nlohmann::json::parse("[[[1, 2, 4], [1, 2, 4]], [[3, 6, 8], [10, 20, 40]]]");
  std::vector<int> Data{1, 3, 1 , 10, 2, 6, 2, 20, 4, 8, 4, 40};
  MultiVector<int> ExpectedResult({2, 2, 3});
  std::copy(Data.begin(), Data.end(), ExpectedResult.begin());
  EXPECT_EQ(ExpectedResult, HDFOperations::jsonArrayToMultiArray<int>(JsonObj));
}

TEST(JsonArrayToMultiVector, Array5) {
  auto JsonObj = nlohmann::json::parse("42");
  std::vector<int> Data{42};
  MultiVector<int> ExpectedResult({1});
  std::copy(Data.begin(), Data.end(), ExpectedResult.begin());
  EXPECT_EQ(ExpectedResult, HDFOperations::jsonArrayToMultiArray<int>(JsonObj));
}