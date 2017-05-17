#pragma once
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <utility>
#include <vector>

// note: this implementation does not disable this overload for array types
template <typename T, typename... TX>
std::unique_ptr<T> make_unique(TX &&... tx) {
  return std::unique_ptr<T>(new T(std::forward<TX>(tx)...));
}

std::vector<char> gulp(std::string fname);

std::vector<char> binary_to_hex(char const *data, int len);

std::vector<std::string> split(std::string const &input, std::string token);

struct get_json_ret_int {
  operator bool() const;
  operator int() const;
  int err;
  int v;
};

struct get_json_ret_object {
  operator bool() const;
  int err = 1;
  rapidjson::Value const *v = nullptr;
};

std::string get_string(rapidjson::Value const *v, std::string path);
get_json_ret_int get_int(rapidjson::Value const *v, std::string path);
get_json_ret_object get_object(rapidjson::Value const &v, std::string path);
std::string pretty_print(rapidjson::Document const *v);

template <typename T>
std::pair<short int, T> to_num(const std::string &string) {
  fprintf(stderr, "Error: can't deduce type in string-to-number conversion.\n");
  return std::pair<short int, T>(false, 0);
}

template <> std::pair<short int, double> to_num(const std::string &string);
template <> std::pair<short int, int> to_num(const std::string &string);
