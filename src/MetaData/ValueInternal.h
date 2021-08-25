// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <functional>
#include <nlohmann/json.hpp>
#include <string>
#include "logger.h"
#include <h5cpp/hdf5.hpp>

namespace MetaDataInternal {
class ValueBaseInternal {
public:
  ValueBaseInternal(std::string const &LocationPath, std::string const &ValueName) : Path(LocationPath), Name(ValueName) {}
  virtual ~ValueBaseInternal() = default;
  virtual nlohmann::json getAsJSON() const = 0;
  virtual void writeToHDF5File(hdf5::node::Node) = 0;
  std::string getName() const { return Name; }
  std::string getPath() const { return Path; }

private:
  std::string Path;
  std::string Name;
};

template <class DataType> class ValueInternal : public ValueBaseInternal {
public:
  ValueInternal(std::string const &LocationPath, std::string const &Name,
                std::function<void(hdf5::node::Node, std::string, DataType)> HDF5Writer)
      : ValueBaseInternal(LocationPath, Name), WriteToFile(HDF5Writer) {}
  void setValue(DataType NewValue) { MetaDataValue = NewValue; }
  DataType getValue() { return MetaDataValue; }
  virtual nlohmann::json getAsJSON() const override {
    nlohmann::json RetObj;
    RetObj[getName()] = MetaDataValue;
    return RetObj;
  }
  virtual void writeToHDF5File(hdf5::node::Node RootNode) override {
    try {
      auto UsedNode = get_node(RootNode, getPath());
      WriteToFile(UsedNode, getName(), MetaDataValue);
    } catch (std::exception &E) {
      LOG_ERROR("Failed to write the value \"{}\" to the path \"{}\" in HDF5-file. The message was: {}", MetaDataValue, getPath(), E.what());
      throw;
    }
  };

private:
  DataType MetaDataValue;
  std::function<void(hdf5::node::Node, std::string, DataType)> WriteToFile;
};
} // namespace MetaDataInternal