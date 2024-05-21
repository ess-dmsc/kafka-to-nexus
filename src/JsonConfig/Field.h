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
  virtual void setValue(std::string const &key, json const &newValue) = 0;
  virtual void setValue(std::string const &Key,
                        std::string const &NewValue) = 0;
  [[nodiscard]] bool hasDefaultValue() const { return got_default; }
  [[nodiscard]] auto getKeys() const { return FieldKeys; }
  [[nodiscard]] bool isRequried() const { return FieldRequired; }

protected:
  bool got_default{true};
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

  void setValue(std::string const &, json const &) override {
    LOG_WARN(
        R"(The field with the key(s) "{}" is obsolete. Any value set will be ignored.)",
        getKeys());
  }

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
  Field(FieldRegistrarType *registrar_ptr, std::vector<KeyString> const &keys,
        FieldType const &value)
      : FieldBase(registrar_ptr, keys), value_(value) {}

  // cppcheck-suppress functionStatic
  template <class FieldRegistrarType>
  Field(FieldRegistrarType *registrar_ptr, KeyString const &key,
        FieldType const &value)
      : FieldBase(registrar_ptr, key), value_(value) {}

  void setValue(const std::string &key, const json &value) override {
    if (value.is_string()) {
      set_value_impl(key, value.get<std::string>());
    } else {
      set_value_impl(key, value);
    }
  }

  void setValue(std::string const &key, std::string const &value) override {
    try {
      set_value_impl(key, json::parse(value));
    } catch (json::exception const &) {
      set_value_impl(key, value);
    }
  }

  FieldType get_value() const { return value_; }

  operator FieldType() const { return value_; }

  [[nodiscard]] std::string get_key() const { return used_key_; }

private:
  FieldType value_;
  std::string used_key_;

  void set_value_impl(std::string const &key, nlohmann::json const &json) {
    if constexpr (std::is_same_v<std::string, FieldType>) {
      try {
        set_value_internal(key, json.get<FieldType>());
      } catch (json::exception const &) {
        // FieldType is a string but
        set_value_internal(key, json.dump());
      }
    } else {
      set_value_internal(key, json.get<FieldType>());
    }
  }

  void set_value_internal(std::string const &key, FieldType value) {
    if (!got_default) {
      auto keys = getKeys();
      auto keys_str =
          std::accumulate(std::next(keys.begin()), keys.end(), keys[0],
                          [](auto a, auto b) { return a + ", " + b; });
      LOG_WARN(
          R"(Replacing the previously given value of "{}" with "{}" in json config field with key(s): )",
          value_, value, keys_str);
    }
    used_key_ = key;
    got_default = false;
    value_ = value;
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
