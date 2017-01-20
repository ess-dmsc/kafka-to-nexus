#include "json.h"
//#define RAPIDJSON_HAS_CXX11_RVALUE_REFS true
//#define RAPIDJSON_HAS_STDSTRING true
#include <rapidjson/document.h>
#include <rapidjson/schema.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <rapidjson/prettywriter.h>

std::string json_to_string(rapidjson::Document const & jd) {
	using namespace rapidjson;
	StringBuffer b1;
	PrettyWriter<StringBuffer> w(b1);
	jd.Accept(w);
	return b1.GetString();
}
