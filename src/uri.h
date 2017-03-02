#pragma once

#include <string>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

#if HAVE_GTEST
#include <gtest/gtest.h>
#endif

namespace BrightnESS {
namespace uri {

class URI {
public:
using uchar = unsigned char;
URI(std::string uri);
bool is_kafka_with_topic() const;
std::string scheme;
std::string host;
uint32_t port = 0;
std::string path;
std::string topic;
static pcre2_code * re1;
static pcre2_code * re_no_host;
static pcre2_code * re_topic;
static bool compile();
static bool compiled;
};

}
}
