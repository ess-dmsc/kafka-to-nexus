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
#include "NeXusDataset/ExtensibleDataset.h"


namespace MetaData {

template<class DataType>
void basicAttributeWriter(hdf5::node::Node Node, std::string Name, DataType Value) {
  if (Node.attributes.exists(Name)) {
    throw std::runtime_error(fmt::format("Unable to create attribute \"{}\" at path \"{}\" as it already exists.", Name, std::string(Node.link().path())));
  }
  Node.attributes.create<DataType>(Name).write(Value);
}

template<class DataType>
void basicDatasetWriter(hdf5::node::Node Node, std::string Name, DataType Value) {
  if (is_dataset(Node)) {
    throw std::runtime_error(fmt::format("Unable to create dataset \"{}\" at path \"{}\" as destination is a dataset.", Name, std::string(Node.link().path())));
  }
  if (is_group(Node) and hdf5::node::Group(Node).has_dataset(Name)) {
    throw std::runtime_error(fmt::format("Unable to create dataset \"{}\" at path \"{}\" as it already exists.", Name, std::string(Node.link().path())));
  }
  auto Dataset = NeXusDataset::ExtensibleDataset<DataType>(Node, Name, NeXusDataset::Mode::Create);
  Dataset.appendElement(Value);
}

}
