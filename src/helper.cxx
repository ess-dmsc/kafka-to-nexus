#include "helper.h"
#include <fstream>
#include <unistd.h>

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
