#include "helper.h"
#include <fstream>
#include <unistd.h>
#include <array>
#include <vector>
#include <string>

std::vector<char> gulp(std::string fname) {
	std::vector<char> ret;
	std::ifstream ifs(fname, std::ios::binary | std::ios::ate);
	if (!ifs.good()) return ret;
	auto n1 = ifs.tellg();
	if (n1 <= 0) return ret;
	ret.resize(n1);
	ifs.seekg(0);
	ifs.read(ret.data(), n1);
	return ret;
}

std::vector<char> binary_to_hex(char const * data, int len) {
	std::vector<char> ret;
	ret.reserve(len * 5 / 4);
	for (int i1 = 0; i1 < len; ++i1) {
		auto c = (uint8_t)data[i1];
		for (auto & v : std::array<uint8_t, 2>{ {(uint8_t)(c >> 4), (uint8_t)(c & 0x0f)} }) {
			if (v < 10) v += 48;
			else v += 97 - 10;
			ret.push_back(v);
		}
		if (i1 %    8  == 7) ret.push_back(' ');
		if (i1 % (4*8) == 4*8-1) ret.push_back('\n');
	}
	return ret;
}

std::vector<std::string> split(std::string const & input, std::string token) {
	using std::vector;
	using std::string;
	vector<string> ret;
	if (token.size() == 0) return { input };
	string::size_type i1 = 0;
	while (true) {
		auto i2 = input.find(token, i1);
		if (i2 == string::npos) break;
		if (i2 > i1) {
			ret.push_back(input.substr(i1, i2-i1));
		}
		i1 = i2 + 1;
	}
	if (i1 != input.size()) {
		ret.push_back(input.substr(i1));
	}
	return ret;
}

#if HAVE_GTEST
#include <gtest/gtest.h>

TEST(helper, split_01) {
	using std::vector;
	using std::string;
	auto v = split("", "");
	ASSERT_EQ(v, vector<string>({""}));
}

TEST(helper, split_02) {
	using std::vector;
	using std::string;
	auto v = split("abc", "");
	ASSERT_EQ(v, vector<string>({"abc"}));
}

TEST(helper, split_03) {
	using std::vector;
	using std::string;
	auto v = split("a/b", "/");
	ASSERT_EQ(v, vector<string>({"a", "b"}));
}

TEST(helper, split_04) {
	using std::vector;
	using std::string;
	auto v = split("/a/b", "/");
	ASSERT_EQ(v, vector<string>({"a", "b"}));
}

TEST(helper, split_05) {
	using std::vector;
	using std::string;
	auto v = split("ac/dc/", "/");
	ASSERT_EQ(v, vector<string>({"ac", "dc"}));
}

TEST(helper, split_06) {
	using std::vector;
	using std::string;
	auto v = split("/ac/dc/", "/");
	ASSERT_EQ(v, vector<string>({"ac", "dc"}));
}

TEST(helper, split_07) {
	using std::vector;
	using std::string;
	auto v = split("/some/longer/thing/for/testing", "/");
	ASSERT_EQ(v, vector<string>({"some", "longer", "thing", "for", "testing"}));
}

#endif
