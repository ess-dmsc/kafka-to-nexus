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
    // Invariant: `it1` is always a iterator into the members of a json object,
    // and that json object is part of the output document which is later
    // returned from this function.
    Value::ConstMemberIterator it1;
    // Invariant: `v1` is always a json object and part of the output document
    // which is later returned from this function.
    Value *v1;
    // Invariant: `v2` is always a json object and part of the 2nd input
    // document to this function.
    Value const *v2;
    // Remember the elements to be copied from the 2nd input document into the
    // document `ret` to be returned.
    deque<to_copy_element> to_copy;
  };
  deque<stack_element> stack;
  stack.push_back(stack_element{ret.MemberBegin(), &ret, &v2});

  while (stack.size() > 0) {
    auto &se = stack.back();
    if (se.it1 != se.v1->MemberEnd()) {
      auto &first_doc_member_name = se.it1->name;
      auto &first_doc_member_value = se.it1->value;
      // Let's see if the 2nd input document has a member of the same name. If
      // not, there is nothing to do.
      auto it2 = se.v2->FindMember(se.it1->name.GetString());
      if (it2 != se.v2->MemberEnd()) {
        auto &second_doc_member_value = it2->value;
        if (first_doc_member_value.IsObject()) {
          if (second_doc_member_value.IsObject()) {
            // If both 1st and 2nd input are json objects at this level then we
            // recursively continue the comparison.
            stack_element e{first_doc_member_value.MemberBegin(),
                            &se.v1->operator[](first_doc_member_name.GetString()),
                            &second_doc_member_value,
                            {}};
            stack.push_back(e);
          } else {
            // it2->value is not a json object, even though it1->value is.
            // We therefore remember that we want to use it to replace whatever
            // is currently stored at the key se.it1->name.
            se.to_copy.push_back({first_doc_member_name.GetString(), &second_doc_member_value});
          }
        } else {
          // it1->value is not a json object.
          // We therefore remember that we want to use it2->value to replace
          // what is currently stored at the key se.it1->name.
          se.to_copy.push_back({first_doc_member_name.GetString(), &second_doc_member_value});
        }
      }
      // Advance the iterator over the elements at the current level of the 1st
      // input document.
      ++se.it1;
    } else {
      // Go through the to-do list of items that we have to copy from the 2nd
      // input document.
      // First erase the existing key in the output.
      // Then copy over the new one.
      for (auto &ce : se.to_copy) {
        se.v1->EraseMember(ce.name.c_str());
        se.v1->AddMember(Value(ce.name.c_str(), a), Value().CopyFrom(*ce.v, a),
                         a);
      }
      // Copy over any key from 2nd input document which did not yet exist in
      // the output.
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
