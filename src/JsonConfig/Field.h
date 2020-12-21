// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "logger.h"
#include <nlohmann/json.hpp>
#include <string>

namespace JsonConfig {

// \brief String class that removes the iterator constructor from std::string.
//
// \note Due to (serious) bugs caused by the accidental misuse of std::string,
// this class inherits from std::string but does not expose a constructor
// that takes a start and an end iterator.
class KeyString : public std::string {
public:
  KeyString(std::string const &Str) // cppcheck-suppress noExplicitConstructor
      : std::string(Str) {}
  KeyString(const char *Ptr) // cppcheck-suppress noExplicitConstructor
      : std::string(Ptr) {}
  KeyString(const char *Ptr, size_t Size) : std::string(Ptr, Size) {}
};

class FieldHandler;

using namespace nlohmann;

class FieldBase {
public:
  FieldBase(FieldHandler *HandlerPtr, std::vector<KeyString> const &Keys);
  FieldBase(FieldHandler *HandlerPtr, KeyString const &Key)
      : FieldBase(HandlerPtr, std::vector<KeyString>{Key}) {}
  virtual ~FieldBase() {}
  virtual void setValue(std::string const &NewValue) = 0;
  [[nodiscard]] bool hasDefaultValue() const { return GotDefault; }
  [[nodiscard]] auto getKeys() const { return FieldKeys; }
  [[nodiscard]] bool isRequried() const { return FieldRequired; }

protected:
  bool GotDefault{true};
  void makeRequired();

private:
  std::vector<std::string> FieldKeys;
  bool FieldRequired{false};
};

template <class FieldType> class Field : public FieldBase {
public:
  Field(FieldHandler *HandlerPtr, std::vector<KeyString> Keys,
        FieldType DefaultValue)
      : FieldBase(HandlerPtr, Keys), FieldValue(DefaultValue) {}
  Field(FieldHandler *HandlerPtr, KeyString const &Key, FieldType DefaultValue)
      : FieldBase(HandlerPtr, Key), FieldValue(DefaultValue) {}

  void setValue(std::string const &ValueString) override {
    setValueImpl<FieldType>(ValueString);
  }

  FieldType getValue() { return FieldValue; };

  operator FieldType() const { return FieldValue; }

protected:
  FieldType FieldValue;
  using FieldBase::makeRequired;

private:
  template <typename T,
            std::enable_if_t<!std::is_same_v<std::string, T>, bool> = true>
  void setValueImpl(std::string const &ValueString) {
    auto JsonData = json::parse(ValueString);
    setValueInternal(JsonData.get<FieldType>());
  }

  template <typename T,
            std::enable_if_t<std::is_same_v<std::string, T>, bool> = true>
  void setValueImpl(std::string const &ValueString) {
    try {
      auto JsonData = json::parse(ValueString);
      setValueInternal(JsonData.get<FieldType>());
    } catch (json::exception const &) {
      setValueInternal(ValueString);
    }
  }
  void setValueInternal(FieldType NewValue) {
    if (not GotDefault) {
      auto Keys = getKeys();
      auto AllKeys =
          std::accumulate(std::next(Keys.begin()), Keys.end(), Keys[0],
                          [](auto a, auto b) { return a + ", " + b; });
      LOG_WARN("Replacing the previously given value of \"{}\" with \"{}\" in "
               "json config field with key(s): ",
               FieldValue, NewValue, AllKeys);
    }
    GotDefault = false;
    FieldValue = NewValue;
  }
};

template <class FieldType> class RequiredField : public Field<FieldType> {
public:
  RequiredField(FieldHandler *HandlerPtr, std::vector<KeyString> const &Keys)
      : Field<FieldType>(HandlerPtr, Keys, FieldType()) {
    FieldBase::makeRequired();
  }
  RequiredField(FieldHandler *HandlerPtr, char const *const StrPtr)
      : RequiredField(HandlerPtr, std::string(StrPtr)) {}
  RequiredField(FieldHandler *HandlerPtr, KeyString const &Key)
      : Field<FieldType>(HandlerPtr, Key, FieldType()) {
    FieldBase::makeRequired();
  }
};

} // namespace JsonConfig
