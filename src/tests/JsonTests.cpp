// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "json.h"
#include <gtest/gtest.h>

using nlohmann::json;

TEST(json, givenFloatGettingAsIntWillUnfortunatelySucceed) {
  auto Doc = json::parse(R"""({"float": 2.0})""");
  auto const &Element = Doc["float"];
  try {
    ASSERT_EQ(2, Element.get<int64_t>());
  } catch (...) {
    bool TheGetDoesUnfortunatelyNotThrow = false;
    ASSERT_TRUE(TheGetDoesUnfortunatelyNotThrow);
  }
}

TEST(json, givenIntegerAsStringGettingAsIntWillFail) {
  auto Doc = json::parse(R"""({"integer_string": "123"})""");
  auto const &Element = Doc["integer_string"];
  try {
    Element.get<int64_t>();
    bool TheGetShouldHaveThrown = false;
    ASSERT_TRUE(TheGetShouldHaveThrown);
  } catch (...) {
  }
}

TEST(json, givenLargeUnsignedIntegerGettingAsSignedUnfortunatelySucceeds) {
  // 9223372036854775807
  // 18446744073709551615
  auto Doc = json::parse(R"""({"value": 18446744073709551615})""");
  auto const &Element = Doc["value"];
  ASSERT_TRUE(Element.is_number_unsigned());
  ASSERT_TRUE(Element.is_number_integer());
  ASSERT_EQ(0xffffffffffffffffu, Element.get<uint64_t>());
  ASSERT_EQ(-1, Element.get<int64_t>());
}

TEST(json, givenTooLargeUnsignedJsonInputFails) {
  // 9223372036854775807
  // 18446744073709551615
  auto Doc = json::parse(R"""({"value": 18446744073709551616})""");
  ASSERT_TRUE(Doc.is_object());
  auto const &Element = Doc["value"];
  ASSERT_FALSE(Element.is_number_unsigned());
  ASSERT_FALSE(Element.is_number_integer());
  ASSERT_TRUE(Element.is_number());
  ASSERT_TRUE(Element.is_number_float());
}
