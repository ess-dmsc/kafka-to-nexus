#include "../helper.h"
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>
#include <gtest/gtest.h>

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
	ASSERT_TRUE(v == vector<string>({"some", "longer", "thing", "for", "testing"}));
}


using namespace rapidjson;

TEST(helper, get_string_01) {
	Document d;
	d.SetObject();
	auto & a = d.GetAllocator();
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
	ASSERT_EQ(s1, "s1");
	s1 = get_string(&d, "mem01.mem10");
	ASSERT_EQ(s1, "s2");
	s1 = get_string(&d, "mem02.1");
	ASSERT_EQ(s1, "something_a_1");
}

TEST(helper, get_string_02) {
	Document d;
	d.Parse("{\"a\":{\"aa\":\"text\",\"bb\":unquoted}}");
	ASSERT_TRUE(d.HasParseError());
}
