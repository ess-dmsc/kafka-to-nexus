#include "logger.h"
#include <cstdlib>
#include <cstdio>
#include <cstdarg>
#include <cstring>
#include <thread>

int log_level = 3;

int prefix_len() {
	// Yes, depends on the name of this very file without the path:
	static int n1 = strlen(__FILE__) - strlen("logger.cxx");
	return n1;
}
