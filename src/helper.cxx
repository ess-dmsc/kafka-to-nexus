#include "helper.h"
#include <fstream>
#include <unistd.h>
#include <array>

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
