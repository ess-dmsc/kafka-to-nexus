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

class FieldHandler;

using namespace nlohmann;

class FieldBase {
public:
  FieldBase(FieldHandler *HandlerPtr, std::vector<std::string> const &Keys);
  FieldBase(FieldHandler *HandlerPtr, std::string const &Key)
      : FieldBase(HandlerPtr, std::vector<std::string>{Key}) {}
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
  Field(FieldHandler *HandlerPtr, std::string const &Key,
        FieldType DefaultValue)
      : FieldBase(HandlerPtr, Key), FieldValue(DefaultValue) {}
  Field(FieldHandler *HandlerPtr, std::vector<std::string> Keys,
        FieldType DefaultValue)
      : FieldBase(HandlerPtr, Keys), FieldValue(DefaultValue) {}

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
               "writer module config field with key(s): ",
               FieldValue, NewValue, AllKeys);
    }
    GotDefault = false;
    FieldValue = NewValue;
  }
};

template <class FieldType> class RequiredField : public Field<FieldType> {
public:
  RequiredField(FieldHandler *HandlerPtr, std::string const &Key)
      : Field<FieldType>(HandlerPtr, Key, FieldType()) {
    FieldBase::makeRequired();
  }
};

} // namespace JsonConfig
