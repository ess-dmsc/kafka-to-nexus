#pragma once

#include "HDF5Storage.h"
#include "ValueInternal.h"
#include <functional>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>

namespace MetaData {

class Tracker;

class ValueBase {
public:
  explicit ValueBase(
      std::shared_ptr<MetaDataInternal::ValueBaseInternal> ValuePtr)
      : ValueObj(ValuePtr) {}
  nlohmann::json getAsJSON() const { return ValueObj->getAsJSON(); }
  std::string getKey() const { return ValueObj->getKey(); }

protected:
  auto getValuePtr() const { return ValueObj; }

private:
  std::shared_ptr<MetaDataInternal::ValueBaseInternal> ValueObj;
  friend Tracker;
};

template <class DataType> class Value : public ValueBase {
public:
  Value(std::string const &Key,
        std::function<void(hdf5::node::Group, DataType)> HDF5Writer = {})
      : ValueBase(std::make_shared<MetaDataInternal::ValueInternal<DataType>>(
            Key, HDF5Writer)) {}
  void setValue(DataType NewValue) {
    std::dynamic_pointer_cast<MetaDataInternal::ValueInternal<DataType>>(
        getValuePtr())
        ->setValue(NewValue);
  }
  DataType getValue() const {
    return std::dynamic_pointer_cast<MetaDataInternal::ValueInternal<DataType>>(
               getValuePtr())
        ->getValue();
  }
  std::string getKey() const { return getKey(); }
};

} // namespace MetaData