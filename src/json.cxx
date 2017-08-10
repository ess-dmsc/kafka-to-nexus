#include "json.h"
#include "logger.h"
#include <deque>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/schema.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <string>

std::string json_to_string(rapidjson::Document const &jd) {
  using namespace rapidjson;
  StringBuffer b1;
  PrettyWriter<StringBuffer> w(b1);
  jd.Accept(w);
  return b1.GetString();
}

/// Merge the json structure v2 into a copy of v1.
/// If the new key in v2 is not a object, it replaces the key in v1.
/// Elements of arrays are also not merged, but the new array fully replaces
/// the old one.
rapidjson::Document merge(rapidjson::Value const &v1,
                          rapidjson::Value const &v2) {
  using namespace rapidjson;
  using std::string;
  using std::deque;
  Document ret;
  auto &a = ret.GetAllocator();
  ret.CopyFrom(v1, a);
  if (!v1.IsObject() || !v2.IsObject()) {
    return ret;
  }
  struct to_copy_element {
    string name;
    Value const *v;
  };
  struct stack_element {
    Value::ConstMemberIterator it1;
    Value *v1;
    Value const *v2;
    deque<to_copy_element> to_copy;
  };
  deque<stack_element> stack;
  stack.push_back(stack_element{ret.MemberBegin(), &ret, &v2});

  while (stack.size() > 0) {
    auto &se = stack.back();
    if (se.it1 != se.v1->MemberEnd()) {
      auto it2 = se.v2->FindMember(se.it1->name.GetString());
      if (it2 != se.v2->MemberEnd()) {
        if (se.it1->value.IsObject()) {
          if (it2->value.IsObject()) {
            stack.push_back(
                stack_element{se.it1->value.MemberBegin(),
                              &se.v1->operator[](se.it1->name.GetString()),
                              &it2->value,
                              {}});
          } else {
            se.to_copy.push_back({se.it1->name.GetString(), &it2->value});
          }
        } else {
          se.to_copy.push_back({se.it1->name.GetString(), &it2->value});
        }
      }
      ++se.it1;
    } else {
      for (auto &ce : se.to_copy) {
        se.v1->EraseMember(ce.name.c_str());
        se.v1->AddMember(Value(ce.name.c_str(), a), Value().CopyFrom(*ce.v, a),
                         a);
      }
      for (auto it2 = se.v2->MemberBegin(); it2 != se.v2->MemberEnd(); ++it2) {
        if (se.v1->FindMember(it2->name) == se.v1->MemberEnd()) {
          se.v1->AddMember(Value(it2->name, a), Value().CopyFrom(it2->value, a),
                           a);
        }
      }
      stack.pop_back();
    }
  }
  return ret;
}
