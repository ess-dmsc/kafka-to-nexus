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

namespace WriterModule {
class Base;
}

namespace WriterModuleConfig {

using namespace nlohmann;

class FieldBase {
public:
  FieldBase(WriterModule::Base *Ptr, std::vector<std::string> Keys);
  FieldBase(WriterModule::Base *Ptr, std::string Key)
      : FieldBase(Ptr, std::vector<std::string>{Key}) {}
  virtual ~FieldBase() {}
  virtual void setValue(std::string const &NewValue) = 0;
  bool hasDefaultValue() { return GotDefault; }
  auto getKeys() { return FieldKeys; }
  bool isRequried() { return FieldRequired; }

protected:
  bool GotDefault{true};
  void makeRequired();

private:
  std::vector<std::string> FieldKeys;
  bool FieldRequired{false};
};

template <class FieldType> class Field : public FieldBase {
public:
  Field(WriterModule::Base *WriterPtr, std::string Key, FieldType DefaultValue)
      : FieldBase(WriterPtr, Key), FieldValue(DefaultValue) {}
  Field(WriterModule::Base *WriterPtr, std::vector<std::string> Keys,
        FieldType DefaultValue)
      : FieldBase(WriterPtr, Keys), FieldValue(DefaultValue) {}

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
  RequiredField(WriterModule::Base *WriterPtr, std::string Key)
      : Field<FieldType>(WriterPtr, Key, FieldType()) {
    FieldBase::makeRequired();
  }
};

} // namespace WriterModuleConfig