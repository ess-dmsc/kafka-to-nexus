#include "logger.h"
#include <cstdlib>
#include <cstdio>
#include <cstdarg>
#include <cstring>
#include <thread>

int log_level = 3;

int prefix_len() {
	static int n1 = strlen(__FILE__) - 10;
	return n1;
}
