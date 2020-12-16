// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "MultiVector.h"
#include <gtest/gtest.h>

TEST(MultiVectorTest, Dimensions1) {
  MultiVector<int> TestVector({1});
  EXPECT_EQ(TestVector.size(), 1u);
}

TEST(MultiVectorTest, Dimensions2) {
  MultiVector<int> TestVector({3,4});
  EXPECT_EQ(TestVector.size(), size_t(3*4));
}

TEST(MultiVectorTest, Dimensions3) {
  MultiVector<int> TestVector({3,4,5});
  EXPECT_EQ(TestVector.size(), size_t(3*4*5));
}

TEST(MultiVectorTest, AccessTooFewDims) {
  MultiVector<int> TestVector({3,4,5});
  EXPECT_THROW(TestVector.at({1,2}), std::out_of_range);
}

TEST(MultiVectorTest, AccessTooManyDims) {
  MultiVector<int> TestVector({3,4,5});
  EXPECT_THROW(TestVector.at({0,0,0,0}), std::out_of_range);
}

TEST(MultiVectorTest, AccessOutsideOfRange) {
  MultiVector<int> TestVector({3,4,5});
  EXPECT_THROW(TestVector.at({1,4,1}), std::out_of_range);
}

TEST(MultiVectorTest, AccessWithinRange) {
  MultiVector<int> TestVector({3,4,5});
  EXPECT_NO_THROW(TestVector.at({2,3,4}));
}

TEST(MultiVectorTest, SetAndGet) {
  MultiVector<int> TestVector({3,4,5});
  TestVector.at({2,3,4}) = 42;
  EXPECT_EQ(TestVector.at({2,3,4}), 42);
}

TEST(MultiVectorTest, SetValue1) {
  MultiVector<int> TestVector({2,2});
  TestVector.at({0,0}) = 33;
  TestVector.at({1,1}) = 42;
  EXPECT_EQ(TestVector, std::vector<int>({33,0,0,42}));
}

TEST(MultiVectorTest, SetValue2) {
  MultiVector<int> TestVector({2,2,2});
  TestVector.at({0,0,0}) = 33;
  TestVector.at({1,1,0}) = 3;
  TestVector.at({0,1,1}) = 2;
  TestVector.at({1,0,1}) = 1;
  TestVector.at({1,1,1}) = 42;
  EXPECT_EQ(TestVector, std::vector<int>({33,0,0,3,0,1,2,42}));
}

TEST(MultiVectorTest, SetValue3) {
  MultiVector<int> TestVector({3,3,3,3});
  TestVector.at({0,0,0,0}) = 11;
  TestVector.at({2,2,2,2}) = 44;
  std::vector<int> Comparison(3*3*3*3);
  Comparison.at(0) = 33;
  *std::prev(Comparison.end()) = 44;
  EXPECT_EQ(TestVector, Comparison);
}
