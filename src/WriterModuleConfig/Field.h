// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <string>
#include "WriterModuleBase.h"
#include <nlohmann/json.hpp>

namespace WriterModule {
class Base;
}

namespace WriterModuleConfig {

using namespace nlohmann;

class FieldBase {
public:
  FieldBase(WriterModule::Base *, std::string Key, bool Required) : FieldKeys{Key}, FieldRequired(Required) {}
  FieldBase(WriterModule::Base *, std::vector<std::string> Keys, bool Required) : FieldKeys{Keys}, FieldRequired(Required) {}
  virtual ~FieldBase() {}
  virtual void setValue(std::string const &NewValue) = 0;
  bool hasDefaultValue() {return GotDefault;}
  auto getKeys() {return FieldKeys;}
  bool isRequried() {return FieldRequired;}
protected:
  bool GotDefault{true};
  void makeRequired();
private:
  std::vector<std::string> FieldKeys;
  bool FieldRequired;
};

template <class FieldType>
class Field : public FieldBase {
public:
  Field(WriterModule::Base *WriterPtr, std::string Key, FieldType DefaultValue) : FieldBase(WriterPtr, Key, false), FieldValue(DefaultValue) {}

  void setValue(std::string const &ValueString) override {
    setValueImpl<FieldType>(ValueString);
  }

  FieldType getValue() {return FieldValue;};

  operator FieldType() const {return FieldValue;}
protected:
  FieldType FieldValue;
  using FieldBase::makeRequired;
private:
  template <typename T,
      std::enable_if_t<!std::is_same_v<std::string,T>, bool> = true>
  void setValueImpl(std::string const &ValueString) {
    auto JsonData = json::parse(ValueString);
    setValueInternal(JsonData.get<FieldType>());
  }

  template <typename T,
      std::enable_if_t<std::is_same_v<std::string,T>, bool> = true>
  void setValueImpl(std::string const &ValueString) {
    try {
      auto JsonData = json::parse(ValueString);
      setValueInternal(JsonData.get<FieldType>());
    } catch (json::exception const &) {
      setValueInternal(ValueString);
    }
  }
  void setValueInternal(FieldType NewValue) {GotDefault = false; FieldValue = NewValue;}
};

template <class FieldType>
class RequiredField : public Field<FieldType> {
public:
  RequiredField(WriterModule::Base *WriterPtr, std::string Key) : Field<FieldType>(WriterPtr, Key, FieldType()) {
    FieldBase::makeRequired();
  }

};

} // namespace WriterModuleConfig