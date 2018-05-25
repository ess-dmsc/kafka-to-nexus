#include "json.h"
#include "logger.h"
#include <gtest/gtest.h>

using namespace rapidjson;
using nlohmann::json;

TEST(json, MergeWithExactlyOneConflictingKey) {
  Document jd1, jd2;
  jd1.Parse(R""({"k1": "string1"})"");
  jd2.Parse(R""({"k1": "string2"})"");
  auto jd3 = merge(jd1, jd2);
  ASSERT_TRUE(jd3 == jd2);
}

TEST(json, MergeTwoDifferentKeys) {
  Document jd1, jd2, jde;
  jd1.Parse(R""({"k1": "string1"})"");
  jd2.Parse(R""({"k2": "string2"})"");
  jde.Parse(R""({
  "k1": "string1",
  "k2": "string2"
})"");
  auto jd3 = merge(jd1, jd2);
  ASSERT_TRUE(jd3 == jde);
}

TEST(json, Merge3KeysWithMoreComplicatedStructures) {
  Document jd1, jd2, jde;
  jd1.Parse(R""({
  "k1": 500,
  "k3": [ "this", {"is":"more"}, "complicated"]
})"");
  jd2.Parse(R""({
  "k3": null,
  "k2": {
    "some": {
      "more": "stuff"
    }
  },
  "k1": {
    "result": 600
  }
})"");
  jde.Parse(R""({
  "k1": {
    "result": 600
  },
  "k3": null,
  "k2": {
    "some": {
      "more": "stuff"
    }
  }
})"");
  auto jd3 = merge(jd1, jd2);
  ASSERT_TRUE(jd3 == jde);
}

TEST(json, MergeDeeperNestedKeys) {
  Document jd1, jd2, jde;

  jd1.Parse(R""({
  "config": {
    "Spaceball One": {
      "speed": 42.01,
      "crew": ["Dark Helmet", 2, 3, 4]
    },
    "a-subobject-to-be-replaced": {
      "this-will-be-gone": 0
    }
  }
})"");

  jd2.Parse(R""({
  "k2": {
    "some": {
      "more": "stuff"
    }
  },
  "config": {
    "Spaceball One": {
      "course": 123,
      "speed": "ludicrous speed"
    },
    "a-subobject-to-be-replaced": ["replaced-by-this-array"]
  }
})"");

  jde.Parse(R""({
  "config": {
    "a-subobject-to-be-replaced": ["replaced-by-this-array"],
    "Spaceball One": {
      "speed": "ludicrous speed",
      "course": 123,
      "crew": ["Dark Helmet", 2, 3, 4]
    }
  },
  "k2": {
    "some": {
      "more": "stuff"
    }
  }
})"");

  auto jd3 = merge(jd1, jd2);
  ASSERT_TRUE(jd3 == jde);
}

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
