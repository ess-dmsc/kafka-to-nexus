#pragma once
#include <rapidjson/document.h>
#include <stdexcept>
#include <string>

class RapidjsonParseError : public std::runtime_error {
public:
  RapidjsonParseError(std::string msg) : std::runtime_error(msg) {}
};

rapidjson::Document stringToRapidjsonOrThrow(std::string const &JsonString);

std::string json_to_string(rapidjson::Value const &jd);

rapidjson::Document merge(rapidjson::Value const &v1,
                          rapidjson::Value const &v2);
