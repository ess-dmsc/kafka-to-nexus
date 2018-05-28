#include "helper.h"
#include <gtest/gtest.h>

using std::string;
using std::vector;

TEST(helper, Split01) {
  auto v = split("", "");
  ASSERT_TRUE(v == vector<string>({""}));
}

TEST(helper, Split02) {
  auto v = split("abc", "");
  ASSERT_TRUE(v == vector<string>({"abc"}));
}

TEST(helper, Split03) {
  auto v = split("a/b", "/");
  ASSERT_TRUE(v == vector<string>({"a", "b"}));
}

TEST(helper, Split04) {
  auto v = split("/a/b", "/");
  ASSERT_TRUE(v == vector<string>({"a", "b"}));
}

TEST(helper, Split05) {
  auto v = split("ac/dc/", "/");
  ASSERT_TRUE(v == vector<string>({"ac", "dc"}));
}

TEST(helper, Split06) {
  auto v = split("/ac/dc/", "/");
  ASSERT_TRUE(v == vector<string>({"ac", "dc"}));
}

TEST(helper, Split07) {
  auto v = split("/some/longer/thing/for/testing", "/");
  ASSERT_TRUE(v ==
              vector<string>({"some", "longer", "thing", "for", "testing"}));
}
