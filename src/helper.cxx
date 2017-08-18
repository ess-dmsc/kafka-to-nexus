#include "helper.h"
#include <array>
#include <fstream>
#include <unistd.h>

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

std::vector<char> gulp(std::string fname) {
  std::vector<char> ret;
  std::ifstream ifs(fname, std::ios::binary | std::ios::ate);
  if (!ifs.good())
    return ret;
  auto n1 = ifs.tellg();
  if (n1 <= 0)
    return ret;
  ret.resize(n1);
  ifs.seekg(0);
  ifs.read(ret.data(), n1);
  return ret;
}

std::vector<char> binary_to_hex(char const *data, uint32_t len) {
  std::vector<char> ret;
  ret.reserve(len * (64 + 5) / 32 + 32);
  for (uint32_t i1 = 0; i1 < len; ++i1) {
    uint8_t c = ((uint8_t)data[i1]) >> 4;
    if (c < 10)
      c += 48;
    else
      c += 97 - 10;
    ret.emplace_back(c);
    c = 0x0f & (uint8_t)data[i1];
    if (c < 10)
      c += 48;
    else
      c += 97 - 10;
    ret.emplace_back(c);
    if ((0x07 & i1) == 0x7) {
      ret.push_back(' ');
      if ((0x1f & i1) == 0x1f)
        ret.push_back('\n');
    }
  }
  return ret;
}

std::vector<std::string> split(std::string const &input, std::string token) {
  using std::vector;
  using std::string;
  vector<string> ret;
  if (token.size() == 0)
    return {input};
  string::size_type i1 = 0;
  while (true) {
    auto i2 = input.find(token, i1);
    if (i2 == string::npos)
      break;
    if (i2 > i1) {
      ret.push_back(input.substr(i1, i2 - i1));
    }
    i1 = i2 + 1;
  }
  if (i1 != input.size()) {
    ret.push_back(input.substr(i1));
  }
  return ret;
}

bool get_json_ret_string::found() const { return err == 0; }
get_json_ret_string::operator bool() const { return err == 0; }
get_json_ret_string::operator std::string() const { return v; }

bool get_json_ret_int::found() const { return err == 0; }
get_json_ret_int::operator bool() const { return err == 0; }
get_json_ret_int::operator int() const { return v; }
get_json_ret_int::operator int64_t() const { return v; }

bool get_json_ret_uint::found() const { return err == 0; }
get_json_ret_uint::operator bool() const { return err == 0; }
get_json_ret_uint::operator uint64_t() const { return v; }

bool get_json_ret_array::found() const { return err == 0; }
get_json_ret_array::operator bool() const { return err == 0; }

bool get_json_ret_object::found() const { return err == 0; }
get_json_ret_object::operator bool() const { return err == 0; }

get_json_ret_string get_string(rapidjson::Value const *v, std::string path) {
  auto a = split(path, ".");
  uint32_t i1 = 0;
  for (auto &x : a) {
    bool num = true;
    for (char &c : x) {
      if (c < 48 || c > 57) {
        num = false;
        break;
      }
    }
    if (num) {
      if (!v->IsArray()) {
        return {1, ""};
      }
      auto n1 = (uint32_t)strtol(x.c_str(), nullptr, 10);
      if (n1 >= v->Size()) {
        return {1, ""};
      }
      auto &v2 = v->GetArray()[n1];
      if (i1 == a.size() - 1) {
        if (v2.IsString()) {
          return {0, v2.GetString()};
        }
      } else {
        v = &v2;
      }
    } else {
      if (!v->IsObject()) {
        return {1, ""};
      }
      auto it = v->FindMember(x.c_str());
      if (it == v->MemberEnd()) {
        return {1, ""};
      }
      if (i1 == a.size() - 1) {
        if (it->value.IsString()) {
          return {0, it->value.GetString()};
        }
      } else {
        v = &it->value;
      }
    }
    ++i1;
  }
  return {1, ""};
}

get_json_ret_int get_int(rapidjson::Value const *v, std::string path) {
  auto a = split(path, ".");
  uint32_t i1 = 0;
  for (auto &x : a) {
    if (!v->IsObject()) {
      return {1, 0};
    }
    auto it = v->FindMember(x.c_str());
    if (it == v->MemberEnd()) {
      return {1, 0};
    }
    if (i1 == a.size() - 1) {
      if (it->value.IsInt()) {
        return {0, it->value.GetInt()};
      }
    } else {
      v = &it->value;
    }
    ++i1;
  }
  return {1, 0};
}

get_json_ret_uint get_uint(rapidjson::Value const *v, std::string path) {
  auto a = split(path, ".");
  uint32_t i1 = 0;
  for (auto &x : a) {
    if (!v->IsObject()) {
      return {1, 0};
    }
    auto it = v->FindMember(x.c_str());
    if (it == v->MemberEnd()) {
      return {1, 0};
    }
    if (i1 == a.size() - 1) {
      if (it->value.IsUint64()) {
        return {0, it->value.GetUint64()};
      }
    } else {
      v = &it->value;
    }
    ++i1;
  }
  return {1, 0};
}

get_json_ret_array get_array(rapidjson::Value const &v_, std::string path) {
  auto v = &v_;
  get_json_ret_array ret;
  ret.err = 1;
  auto a = split(path, ".");
  uint32_t i1 = 0;
  for (auto &x : a) {
    if (!v->IsObject()) {
      return ret;
    }
    auto it = v->FindMember(x.c_str());
    if (it == v->MemberEnd()) {
      return ret;
    }
    if (i1 == a.size() - 1) {
      if (it->value.IsArray()) {
        ret.err = 0;
        ret.v = &it->value;
        return ret;
      }
    } else {
      v = &it->value;
    }
    ++i1;
  }
  return ret;
}

get_json_ret_object get_object(rapidjson::Value const &v_, std::string path) {
  auto v = &v_;
  get_json_ret_object ret;
  ret.err = 1;
  auto a = split(path, ".");
  uint32_t i1 = 0;
  for (auto &x : a) {
    if (!v->IsObject()) {
      return ret;
    }
    auto it = v->FindMember(x.c_str());
    if (it == v->MemberEnd()) {
      return ret;
    }
    if (i1 == a.size() - 1) {
      if (it->value.IsObject()) {
        ret.err = 0;
        ret.v = &it->value;
        return ret;
      }
    } else {
      v = &it->value;
    }
    ++i1;
  }
  return ret;
}

/// During development, I find it sometimes useful to be able to quickly pretty
/// print some json object.
std::string pretty_print(rapidjson::Document const *v) {
  rapidjson::StringBuffer buf1;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> wr(buf1);
  v->Accept(wr);
  return buf1.GetString();
}

template <> std::pair<short int, int> to_num(const std::string &string) {
  int result;
  try {
    result = std::stoi(string);
  } catch (std::exception &e) {
    return std::pair<short int, int>(false, 0);
  }
  return std::pair<short int, int>(true, result);
}

template <> std::pair<short int, double> to_num(const std::string &string) {
  double result;
  try {
    result = std::stod(string);
  } catch (std::exception &e) {
    return std::pair<short int, double>(false, 0);
  }
  return std::pair<short int, double>(true, result);
}
