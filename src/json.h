#pragma once

#include <rapidjson/document.h>
#include <nlohmann/json.hpp>
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

template <typename T> class JsonMaybe {
public:
  JsonMaybe() {}
  JsonMaybe(T inner) : inner_(inner), found_(true) {}
  explicit operator bool() const { return found_; }
  T inner() const { return inner_; }
private:
  T inner_;
  bool found_ = false;
};

template <typename T> JsonMaybe<T> get(std::string Key, nlohmann::json const &Json);

template <typename T> T get_or(std::string Key, std::function<nlohmann::json()> Else, nlohmann::json const &Json);
