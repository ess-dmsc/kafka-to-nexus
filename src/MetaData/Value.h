// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "ValueInternal.h"
#include <functional>
#include <h5cpp/hdf5.hpp>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>

namespace MetaData {

class Tracker;

class ValueBase {
public:
  explicit ValueBase(
      std::shared_ptr<MetaDataInternal::ValueBaseInternal> ValuePtr)
      : ValueObj(ValuePtr) {}
  nlohmann::json getAsJSON() const {
    throwIfInvalid();
    return ValueObj->getAsJSON();
  }
  std::string getKey() const {
    throwIfInvalid();
    return ValueObj->getName();
  }
  bool isValid() const { return ValueObj != nullptr; }
  void throwIfInvalid() const {
    if (!isValid()) {
      throw std::runtime_error("Unable to set or get meta data value as it has "
                               "not been initialised.");
    }
  }

protected:
  auto getValuePtr() const {
    throwIfInvalid();
    return ValueObj;
  }

private:
  std::shared_ptr<MetaDataInternal::ValueBaseInternal> ValueObj{nullptr};
  friend Tracker;
};

template <class DataType> class Value : public ValueBase {
public:
  Value() = default;
  Value(std::string const &Path, std::string const &Name,
        std::function<void(hdf5::node::Node, std::string, DataType)>
            HDF5Writer = {},
        std::function<void(hdf5::node::Node, std::string, std::string)>
            HDF5AttributeWriter = {})
      : ValueBase(std::make_shared<MetaDataInternal::ValueInternal<DataType>>(
            Path, Name, HDF5Writer, HDF5AttributeWriter)) {}

  Value(char const *const Path, std::string const &Name,
        std::function<void(hdf5::node::Node, std::string, DataType)>
            HDF5Writer = {},
        std::function<void(hdf5::node::Node, std::string, std::string)>
            HDF5AttributeWriter = {})
      : ValueBase(std::make_shared<MetaDataInternal::ValueInternal<DataType>>(
            Path, Name, HDF5Writer, HDF5AttributeWriter)) {}

  template <class NodeType>
  Value(NodeType const &Node, std::string const &Name,
        std::function<void(hdf5::node::Node, std::string, DataType)>
            HDF5Writer = {},
        std::function<void(hdf5::node::Node, std::string, std::string)>
            HDF5AttributeWriter = {})
      : ValueBase(std::make_shared<MetaDataInternal::ValueInternal<DataType>>(
            std::string(Node.link().path()), Name, HDF5Writer,
            HDF5AttributeWriter)) {}

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
  std::optional<std::string> getAttribute(const std::string &Key) const {
    return getValuePtr()->getAttribute(Key);
  }

  // cppcheck-suppress functionConst
  void setAttribute(const std::string &Key, const std::string &Value) {

    getValuePtr()->setAttribute(Key, Value);
  }
};

} // namespace MetaData