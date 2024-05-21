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

/// \brief The base class of JSON key/value pairs used for configuration
/// purposes.
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
  virtual void setValue(std::string const &Key,
                        std::string const &NewValue) = 0;
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

/// \brief Represents an obsolete JSON key/value pair in a dictionary.
///
/// If the key is found, a warning message is produced by the FieldHandler.
/// \tparam FieldType The data type stored in the field. Need not be a primitive
/// type.
template <class FieldType> class ObsoleteField : public FieldBase {
public:
  template <class FieldRegistrarType>
  ObsoleteField(FieldRegistrarType *RegistrarPtr,
                std::vector<KeyString> const &Keys)
      : FieldBase(RegistrarPtr, Keys) {}

  template <class FieldRegistrarType>
  ObsoleteField(FieldRegistrarType *RegistrarPtr, KeyString const &Key)
      : FieldBase(RegistrarPtr, Key) {}

  void setValue(std::string const &, std::string const &) override {
    LOG_WARN(
        R"(The field with the key(s) "{}" is obsolete. Any value set will be ignored.)",
        getKeys());
  }

  FieldType getValue() const {
    throw std::runtime_error(
        R"(Unable to return a value for the field with the key(s) "{}" as it has been made obsolete.)",
        getKeys());
  }

  operator FieldType() const { return getValue(); }
};

/// \brief Represents a JSON key/value pair in a dictionary.
///
/// \tparam FieldType The data type stored in the field. Need not be a primitive
/// type.
template <class FieldType> class Field : public FieldBase {
public:
  template <class FieldRegistrarType>
  Field(FieldRegistrarType *RegistrarPtr, std::vector<KeyString> const &Keys,
        FieldType const &DefaultValue)
      : FieldBase(RegistrarPtr, Keys), FieldValue(DefaultValue) {}

  // cppcheck-suppress functionStatic
  template <class FieldRegistrarType>
  Field(FieldRegistrarType *RegistrarPtr, KeyString const &Key,
        FieldType const &DefaultValue)
      : FieldBase(RegistrarPtr, Key), FieldValue(DefaultValue) {}

  void setValue(std::string const &Key,
                std::string const &ValueString) override {
    setValueImpl<FieldType>(Key, ValueString);
  }

  FieldType getValue() const { return FieldValue; }

  operator FieldType() const { return FieldValue; }

  [[nodiscard]] std::string getUsedKey() const { return UsedKey; }

protected:
  std::string UsedKey;
  FieldType FieldValue;
  using FieldBase::makeRequired;

private:
  template <typename T,
            std::enable_if_t<!std::is_same_v<std::string, T>, bool> = true>
  void setValueImpl(std::string const &Key, std::string const &ValueString) {
    auto JsonData = json::parse(ValueString);
    setValueInternal(Key, JsonData.get<FieldType>());
  }

  template <typename T,
            std::enable_if_t<std::is_same_v<std::string, T>, bool> = true>
  void setValueImpl(std::string const &Key, std::string const &ValueString) {
    try {
      auto JsonData = json::parse(ValueString);
      setValueInternal(Key, JsonData.get<FieldType>());
    } catch (json::exception const &) {
      setValueInternal(Key, ValueString);
    }
  }
  void setValueInternal(std::string const &Key, FieldType NewValue) {
    if (not GotDefault) {
      auto Keys = getKeys();
      auto AllKeys =
          std::accumulate(std::next(Keys.begin()), Keys.end(), Keys[0],
                          [](auto a, auto b) { return a + ", " + b; });
      LOG_WARN(
          R"(Replacing the previously given value of "{}" with "{}" in json config field with key(s): )",
          FieldValue, NewValue, AllKeys);
    }
    UsedKey = Key;
    GotDefault = false;
    FieldValue = NewValue;
  }
};

/// \brief Represents a required JSON key/value pair in a dictionary.
///
/// When processed by the FieldHandler, an exception will be thrown if this
/// field (key) is not found. \tparam FieldType The data type stored in the
/// field. Need not be a primitive type.
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
