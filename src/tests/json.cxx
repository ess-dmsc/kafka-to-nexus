#include "../json.h"
#include "../logger.h"
#include <gtest/gtest.h>

using namespace rapidjson;

TEST(json, merge_01) {
  Document jd1, jd2;
  jd1.Parse(R""({"k1": "string1"})"");
  jd2.Parse(R""({"k1": "string2"})"");
  auto jd3 = merge(jd1, jd2);
  LOG(9, "{}", json_to_string(jd3));
  ASSERT_TRUE(jd3 == jd2);
}

TEST(json, merge_02) {
  Document jd1, jd2, jde;
  jd1.Parse(R""({"k1": "string1"})"");
  jd2.Parse(R""({"k2": "string2"})"");
  jde.Parse(R""({
  "k1": "string1",
  "k2": "string2"
})"");
  auto jd3 = merge(jd1, jd2);
  LOG(9, "{}", json_to_string(jd3));
  ASSERT_TRUE(jd3 == jde);
}

TEST(json, merge_03) {
  Document jd1, jd2, jde;
  jd1.Parse(R""({
  "k1": "string1"
})"");
  jd2.Parse(R""({
  "k2": {
    "some": {
      "more": "stuff"
    }
  },
  "k1": "string1"
})"");
  jde.Parse(R""({
  "k1": "string1",
  "k2": {
    "some": {
      "more": "stuff"
    }
  }
})"");
  auto jd3 = merge(jd1, jd2);
  LOG(9, "{}", json_to_string(jd3));
  ASSERT_TRUE(jd3 == jde);
}

TEST(json, merge_04) {
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
  LOG(9, "expected: {}", json_to_string(jde));
  LOG(9, "returned: {}", json_to_string(jd3));
  ASSERT_TRUE(jd3 == jde);
}
