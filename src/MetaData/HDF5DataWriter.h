// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "NeXusDataset/ExtensibleDataset.h"
#include <functional>

namespace MetaData {

template <class DataType>
void basicAttributeWriter(hdf5::node::Node Node, std::string Name,
                          DataType Value) {
  if (Node.attributes.exists(Name)) {
    throw std::runtime_error(fmt::format(R"(Unable to create attribute "{}" at path "{}" as it already exists.)",
                                         Name,
                                         std::string(Node.link().path())));
  }
  Node.attributes.create<DataType>(Name).write(Value);
}

template <class DataType>
std::function<void(hdf5::node::Node, std::string, DataType)>
getPathOffsetAttributeWriter(std::string PathOffset) {
  auto TempFunction = [PathOffset](hdf5::node::Node Node, std::string Name,
                                   DataType Value) {
    auto UsedNode =
        hdf5::node::Node(hdf5::node::Group(Node).get_dataset(PathOffset));
    MetaData::basicAttributeWriter<DataType>(UsedNode, Name, Value);
  };
  return TempFunction;
}

template <class DataType>
void basicDatasetWriter(hdf5::node::Node Node, std::string Name,
                        DataType Value) {
  if (is_dataset(Node)) {
    throw std::runtime_error(
        fmt::format(R"(Unable to create dataset "{}" at path "{}" as destination is a dataset.)",
                    Name, std::string(Node.link().path())));
  }
  if (is_group(Node) and hdf5::node::Group(Node).has_dataset(Name)) {
    throw std::runtime_error(fmt::format(
        R"(Unable to create dataset "{}" at path "{}" as it already exists.)",
        Name, std::string(Node.link().path())));
  }
  auto Dataset = NeXusDataset::ExtensibleDataset<DataType>(
      Node, Name, NeXusDataset::Mode::Create);
  Dataset.appendElement(Value);
}

void basicStringDatasetWriter(hdf5::node::Node Node, std::string Name,
                              std::string Value);

template <class DataType>
std::function<void(hdf5::node::Node, std::string, DataType)>
getPathOffsetDatasetWriter(std::string PathOffset) {
  auto TempFunction = [PathOffset](hdf5::node::Node Node, std::string Name,
                                   DataType Value) {
    auto UsedNode =
        hdf5::node::Node(hdf5::node::Group(Node).get_dataset(PathOffset));
    MetaData::basicDatasetWriter<DataType>(UsedNode, Name, Value);
  };
  return TempFunction;
}

} // namespace MetaData
