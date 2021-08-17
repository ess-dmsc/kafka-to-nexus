// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "HDF5Storage.h"
#include <functional>
#include <nlohmann/json.hpp>
#include <string>

namespace MetaDataInternal {
class ValueBaseInternal {
public:
  ValueBaseInternal(std::string const &ValueKey) : Key(ValueKey) {}
  virtual ~ValueBaseInternal() = default;
  virtual nlohmann::json getAsJSON() const = 0;
  virtual void writeToHDF5File(hdf5::node::Group) = 0;
  std::string getKey() const { return Key; }

private:
  std::string Key;
};

template <class DataType> class ValueInternal : public ValueBaseInternal {
public:
  ValueInternal(std::string const &Key,
                std::function<void(hdf5::node::Group, DataType)> HDF5Writer)
      : ValueBaseInternal(Key), WriteToFile(HDF5Writer) {}
  void setValue(DataType NewValue) { MetaDataValue = NewValue; }
  DataType getValue() { return MetaDataValue; }
  virtual nlohmann::json getAsJSON() const override {
    nlohmann::json RetObj;
    RetObj[getKey()] = MetaDataValue;
    return RetObj;
  }
  virtual void writeToHDF5File(hdf5::node::Group) override{};

private:
  DataType MetaDataValue;
  std::function<void(hdf5::node::Group, DataType)> WriteToFile;
};
} // namespace MetaDataInternal