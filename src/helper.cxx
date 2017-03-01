#include "helper.h"
#include "logger.h"
#include <array>
#include <fstream>
#include <unistd.h>

std::vector<char> gulp(std::string fname) {
	std::array<char, 256> cwd;
	getcwd(cwd.data(), cwd.size());
	LOG(3, "gulp: {}  cwd: {}", fname, cwd.data());
	std::vector<char> ret;
	std::ifstream ifs(fname, std::ios::binary | std::ios::ate);
	auto n1 = ifs.tellg();
	LOG(3, "try to resize: {}", n1);
	if (n1 <= 0) return ret;
	ret.resize(n1);
	ifs.seekg(0);
	ifs.read(ret.data(), n1);
	return ret;
}
