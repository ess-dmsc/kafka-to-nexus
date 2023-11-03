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
#include <functional>
#include <h5cpp/hdf5.hpp>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>

namespace StatisticsInternal {
class ValueBaseInternal {
public:
  ValueBaseInternal(
      std::string const &LocationPath, std::string const &ValueName,
      std::function<void(hdf5::node::Node, std::string, std::string)>
          HDF5AttributeWriter)
      : Path(LocationPath), Name(ValueName),
        WriteAttributeToFile(HDF5AttributeWriter) {}
  virtual ~ValueBaseInternal() = default;
  virtual nlohmann::json getAsJSON() const = 0;
  virtual void writeToHDF5File(hdf5::node::Group) const = 0;
  std::string getName() const { return Name; }
  std::string getPath() const { return Path; }
  std::optional<std::string> getAttribute(const std::string &Key) {
    auto it = Attributes.find(Key);
    if (it != Attributes.end())
      return it->second;
    else
      return {};
  }
  void setAttribute(const std::string &Key, const std::string &Value) {
    Attributes[Key] = Value;
  }
  void writeAttributesToHDF5File(hdf5::node::Node Node) const {
    if (WriteAttributeToFile) {
      for (auto const &[Key, Value] : Attributes) {
        WriteAttributeToFile(Node, Key, Value);
      }
    }
  }

private:
  std::string Path;
  std::string Name;
  std::map<std::string, std::string> Attributes;
  std::function<void(hdf5::node::Node, std::string, std::string)>
      WriteAttributeToFile;
};

template <class DataType> class ValueInternal : public ValueBaseInternal {
public:
  ValueInternal(
      std::string const &LocationPath, std::string const &Name,
      std::function<void(hdf5::node::Node, std::string, DataType)> HDF5Writer,
      std::function<void(hdf5::node::Node, std::string, std::string)>
          HDF5AttributeWriter)
      : ValueBaseInternal(LocationPath, Name, HDF5AttributeWriter),
        WriteToFile(HDF5Writer) {}
  void setValue(DataType const &NewValue) {
    std::lock_guard Lock(ValueMutex);
    StatisticValue = NewValue;
  }
  DataType getValue() const {
    std::lock_guard Lock(ValueMutex);
    return StatisticValue;
  }
  virtual nlohmann::json getAsJSON() const override {
    std::lock_guard Lock(ValueMutex);
    nlohmann::json RetObj;
    RetObj[getPath() + ":" + getName()] = StatisticValue;
    return RetObj;
  }
  virtual void writeToHDF5File(hdf5::node::Group RootNode) const override {
    std::lock_guard Lock(ValueMutex);
    try {
      auto UsedNode = get_group(RootNode, getPath());
      if (WriteToFile) {
        WriteToFile(UsedNode, getName(), StatisticValue);
      }
      ValueBaseInternal::writeAttributesToHDF5File(
          UsedNode.get_dataset(getName()));
    } catch (std::exception &E) {
      LOG_ERROR(
          R"(Failed to write the value "{}" to the path "{}" in HDF5-file. The message was: {})",
          StatisticValue, getPath(), E.what());
      throw;
    }
  };

private:
  mutable std::mutex ValueMutex;
  DataType StatisticValue{};
  std::function<void(hdf5::node::Node, std::string, DataType)> WriteToFile;
};
} // namespace StatisticsInternal
