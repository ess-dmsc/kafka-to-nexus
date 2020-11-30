
#include <string>
#include "WriterModuleBase.h"
#include <nlohmann/json.hpp>

namespace WriterModuleConfig {

using namespace nlohmann;

class FieldBase {
public:
  FieldBase(WriterModule::Base *, std::string Key, bool Required) : FieldKey(Key), FieldRequired(Required) {}
  virtual ~FieldBase() {}
  virtual void setValue(std::string const &NewValue) = 0;
  bool hasDefaultValue() {return GotDefault;}
  std::string getKey() {return FieldKey;}
  bool isRequried() {return FieldRequired;}
protected:
  bool GotDefault{true};
private:
  std::string FieldKey;
  bool FieldRequired;
};

template <class FieldType>
class Field : public FieldBase {
public:
  Field(WriterModule::Base *WriterPtr, std::string Key, bool Required, FieldType DefaultValue) : FieldBase(WriterPtr, Key, Required), FieldValue(DefaultValue) {}

  void setValue(std::string const &ValueString) override {
    setValueImpl<FieldType>(ValueString);
  }


  FieldType getValue() {return FieldValue;};
protected:
  FieldType FieldValue;
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

} // namespace WriterModuleConfig