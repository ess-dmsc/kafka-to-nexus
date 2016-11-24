#pragma once

#include <stdarg.h>
#include <fmt/format.h>

// Do we have a cross-platform way of doing this?
// Would a stream work better?
#ifdef _MSC_VER

#define LOG(level, fmt, ...) { \
	if (level >= log_level) { \
		dwlog(level, fmt, __FILE__, __LINE__, __FUNCSIG__, __VA_ARGS__); \
	} \
}

//#define QLOG(level, fmt, ...) { dwlog(level, "[%lx] " fmt, __FILE__, __LINE__, __FUNCSIG__, (uint64_t)QThread::currentThreadId(), __VA_ARGS__); }

#else

#define LOG(level, fmt, args...) { \
	if (level >= log_level) { \
		dwlog(level, fmt, __FILE__, __LINE__, __PRETTY_FUNCTION__, ## args); \
	} \
}

//#define QLOG(level, fmt, args...) { dwlog(level, "[%lx] " fmt, __FILE__, __LINE__, __PRETTY_FUNCTION__, (uint64_t)QThread::currentThreadId(), ## args); }

#endif



extern int log_level;

int prefix_len();

template <typename ...TT>
void dwlog(int level, char const * fmt, char const * file, int line, char const * func, TT const & ... args) {
	// Here, we assume that this file is still called logger.cpp and lives in the root of the project
	int npre = prefix_len();
	int const n2 = strlen(file);
	if (npre > n2) {
		fmt::print("ERROR in logging API: npre > n2\n");
		npre = 0;
	}
	auto f1 = file + npre;
	// TODO merge into one invocation..
	try {
		fmt::print("{}:{}: [{}]  {}\n", f1, line, level, fmt::format(fmt, args...));
	}
	catch (fmt::FormatError & e) {
		fmt::print("ERROR  fmt::FormatError {}:{}\n", f1, line);
	}
}
