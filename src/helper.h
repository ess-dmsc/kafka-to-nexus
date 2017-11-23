#pragma once
#include <chrono>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <thread>
#include <utility>
#include <vector>

void sleep_ms(size_t ms);

uint64_t getpid_wrapper();

// note: this implementation does not disable this overload for array types
template <typename T, typename... TX>
std::unique_ptr<T> make_unique(TX &&... tx) {
  return std::unique_ptr<T>(new T(std::forward<TX>(tx)...));
}

std::vector<char> gulp(std::string fname);

std::vector<char> binary_to_hex(char const *data, uint32_t len);

std::vector<std::string> split(std::string const &input, std::string token);

struct get_json_ret_string {
  explicit operator bool() const;
  explicit operator std::string() const;
  bool found() const;
  int err;
  std::string v;
};

struct get_json_ret_int {
  explicit operator bool() const;
  explicit operator int() const;
  explicit operator int64_t() const;
  bool found() const;
  int err;
  int64_t v;
};

struct get_json_ret_uint {
  explicit operator bool() const;
  explicit operator uint64_t() const;
  bool found() const;
  int err;
  uint64_t v;
};

struct get_json_ret_bool {
  explicit operator bool() const;
  bool found() const;
  int v;
};

struct get_json_ret_array {
  explicit operator bool() const;
  bool found() const;
  int err = 1;
  rapidjson::Value const *v = nullptr;
};

struct get_json_ret_object {
  explicit operator bool() const;
  bool found() const;
  int err = 1;
  rapidjson::Value const *v = nullptr;
};

get_json_ret_string get_string(rapidjson::Value const *v, std::string path);
get_json_ret_int get_int(rapidjson::Value const *v, std::string path);
get_json_ret_uint get_uint(rapidjson::Value const *v, std::string path);
get_json_ret_bool get_bool(rapidjson::Value const *v, std::string path);
get_json_ret_array get_array(rapidjson::Value const &v, std::string path);
get_json_ret_object get_object(rapidjson::Value const &v, std::string path);

std::string pretty_print(rapidjson::Document const *v);

template <typename T>
std::pair<short int, T> to_num(const std::string &string) {
  fprintf(stderr, "Error: can't deduce type in string-to-number conversion.\n");
  return std::pair<short int, T>(false, 0);
}

template <> std::pair<short int, int> to_num(const std::string &string);
template <> std::pair<short int, uint64_t> to_num(const std::string &string);
template <> std::pair<short int, double> to_num(const std::string &string);
