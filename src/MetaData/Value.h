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
#include <string>

namespace MetaData {

class Tracker;

class ValueBase {
public:
  explicit ValueBase(
      std::shared_ptr<MetaDataInternal::ValueBaseInternal> ValuePtr)
      : ValueObj(ValuePtr) {}
  nlohmann::json getAsJSON() const { return ValueObj->getAsJSON(); }
  std::string getKey() const { return ValueObj->getName(); }

protected:
  auto getValuePtr() const { return ValueObj; }

private:
  std::shared_ptr<MetaDataInternal::ValueBaseInternal> ValueObj;
  friend Tracker;
};

template <class DataType> class Value : public ValueBase {
public:
  Value(std::string const &Path, std::string const &Name,
        std::function<void(hdf5::node::Node, std::string, DataType)>
            HDF5Writer = {})
      : ValueBase(std::make_shared<MetaDataInternal::ValueInternal<DataType>>(
            Path, Name, HDF5Writer)) {}

  Value(char const *const Path, std::string const &Name,
        std::function<void(hdf5::node::Node, std::string, DataType)>
            HDF5Writer = {})
      : ValueBase(std::make_shared<MetaDataInternal::ValueInternal<DataType>>(
            Path, Name, HDF5Writer)) {}

  template <class NodeType>
  Value(NodeType const &Node, std::string const &Name,
        std::function<void(hdf5::node::Node, std::string, DataType)>
            HDF5Writer = {})
      : ValueBase(std::make_shared<MetaDataInternal::ValueInternal<DataType>>(
            std::string(Node.link().path()), Name, HDF5Writer)) {}

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