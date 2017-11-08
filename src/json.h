#pragma once
#include <rapidjson/document.h>
#include <string>

std::string json_to_string(rapidjson::Value const &jd);

rapidjson::Document merge(rapidjson::Value const &v1,
                          rapidjson::Value const &v2);
