#include "../helper.h"
#include <gtest/gtest.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

TEST(helper, split_01) {
  using std::vector;
  using std::string;
  auto v = split("", "");
  ASSERT_TRUE(v == vector<string>({""}));
}

TEST(helper, split_02) {
  using std::vector;
  using std::string;
  auto v = split("abc", "");
  ASSERT_TRUE(v == vector<string>({"abc"}));
}

TEST(helper, split_03) {
  using std::vector;
  using std::string;
  auto v = split("a/b", "/");
  ASSERT_TRUE(v == vector<string>({"a", "b"}));
}

TEST(helper, split_04) {
  using std::vector;
  using std::string;
  auto v = split("/a/b", "/");
  ASSERT_TRUE(v == vector<string>({"a", "b"}));
}

TEST(helper, split_05) {
  using std::vector;
  using std::string;
  auto v = split("ac/dc/", "/");
  ASSERT_TRUE(v == vector<string>({"ac", "dc"}));
}

TEST(helper, split_06) {
  using std::vector;
  using std::string;
  auto v = split("/ac/dc/", "/");
  ASSERT_TRUE(v == vector<string>({"ac", "dc"}));
}

TEST(helper, split_07) {
  using std::vector;
  using std::string;
  auto v = split("/some/longer/thing/for/testing", "/");
  ASSERT_TRUE(v ==
              vector<string>({"some", "longer", "thing", "for", "testing"}));
}

using namespace rapidjson;

static Document make_test_doc() {
  Document d;
  d.Parse("{\"a\":{\"aa\":\"text\",\"bb\":456}}");
  return d;
}

TEST(helper, make_test_doc) { ASSERT_FALSE(make_test_doc().HasParseError()); }

TEST(helper, get_string_01) {
  Document d;
  d.SetObject();
  auto &a = d.GetAllocator();
  d.AddMember("mem00", Value("s1", a), a);
  Value v2;
  v2.SetObject();
  v2.AddMember("mem10", Value("s2", a), a);
  d.AddMember("mem01", v2.Move(), a);

  {
    Value va;
    va.SetArray();
    va.PushBack(Value("something_a_0", a), a);
    va.PushBack(Value("something_a_1", a), a);
    va.PushBack(Value(1234), a);
    d.AddMember("mem02", va, a);
  }

  StringBuffer buf1;
  PrettyWriter<StringBuffer> wr(buf1);
  d.Accept(wr);
  auto s1 = get_string(&d, "mem00");
  ASSERT_EQ(s1.v, "s1");
  s1 = get_string(&d, "mem01.mem10");
  ASSERT_EQ(s1.v, "s2");
  s1 = get_string(&d, "mem02.1");
  ASSERT_EQ(s1.v, "something_a_1");
}

TEST(helper, get_string_02) {
  auto d = make_test_doc();
  auto s = get_string(&d, "a.aa");
  ASSERT_EQ(s.v, "text");
}

TEST(helper, get_string_03) {
  Document d;
  d.Parse("{\"a\":{\"aa\":\"text\",\"bb\":unquoted}}");
  ASSERT_TRUE(d.HasParseError());
}

TEST(helper, get_int_01) {
  auto d = make_test_doc();
  auto r = get_int(&d, "a.bb");
  ASSERT_EQ((bool)r, true);
  ASSERT_EQ((int)r, 456);
  if (auto g = get_int(&d, "a.bb")) {
    ASSERT_EQ((int)g, 456);
  } else {
    // We should not be here
    ASSERT_TRUE(false);
  }
  if (get_int(&d, "blabla")) {
    // we should not be here
    ASSERT_TRUE(false);
  }
}

TEST(helper, get_object) {
  Document d;
  d.Parse("{\"a\":{\"b\":{\"k\":\"the object\"}}}");
  auto o1 = get_object(d, "a.b").v;
  auto o2 = get_object(d, "a.b").v;
  ASSERT_EQ(o1, o2);
  ASSERT_EQ(get_string(o1, "k").v, "the object");
  ASSERT_EQ(get_string(o2, "k").v, "the object");
}
