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
  template <class FieldRegistrarType>
  FieldBase(FieldRegistrarType *RegistrarPtr,
            std::vector<KeyString> const &Keys)
      : FieldKeys(Keys.begin(), Keys.end()) {
    RegistrarPtr->registerField(this);
  }

  template <class FieldRegistrarType>
  FieldBase(FieldRegistrarType *RegistrarPtr, KeyString const &Key)
      : FieldBase(RegistrarPtr, std::vector<KeyString>{Key}) {}

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
  template <class FieldRegistrarType>
  Field(FieldRegistrarType *RegistrarPtr, std::vector<KeyString> const &Keys,
        FieldType DefaultValue)
      : FieldBase(RegistrarPtr, Keys), FieldValue(DefaultValue) {}

  template <class FieldRegistrarType>
  Field(FieldRegistrarType *RegistrarPtr, KeyString const &Key,
        FieldType DefaultValue)
      : FieldBase(RegistrarPtr, Key), FieldValue(DefaultValue) {}

  void setValue(std::string const &ValueString) override {
    setValueImpl<FieldType>(ValueString);
  }

  FieldType getValue() const { return FieldValue; }

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
  template <class FieldRegistrarType>
  RequiredField(FieldRegistrarType *RegistrarPtr,
                std::vector<KeyString> const &Keys)
      : Field<FieldType>(RegistrarPtr, Keys, FieldType()) {
    FieldBase::makeRequired();
  }

  template <class FieldRegistrarType>
  RequiredField(FieldRegistrarType *RegistrarPtr, char const *const StrPtr)
      : RequiredField(RegistrarPtr, std::string(StrPtr)) {}

  template <class FieldRegistrarType>
  RequiredField(FieldRegistrarType *RegistrarPtr, KeyString const &Key)
      : Field<FieldType>(RegistrarPtr, Key, FieldType()) {
    FieldBase::makeRequired();
  }
};

} // namespace JsonConfig
