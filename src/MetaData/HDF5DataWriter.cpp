// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDF5DataWriter.h"

namespace MetaData {

void basicStringDatasetWriter(hdf5::node::Node Node, std::string Name,
                              std::string Value) {
  if (is_dataset(Node)) {
    throw std::runtime_error(fmt::format(
        R"(Unable to create dataset "{}" at path "{}" as destination is a dataset.)",
        Name, std::string(Node.link().path())));
  }
  if (is_group(Node) && hdf5::node::Group(Node).has_dataset(Name)) {
    throw std::runtime_error(fmt::format(
        R"(Unable to create dataset "{}" at path "{}" as it already exists.)",
        Name, std::string(Node.link().path())));
  }
  auto string_type = hdf5::datatype::String::variable();
  string_type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  string_type.padding(hdf5::datatype::StringPad::NullTerm);
  auto string_dataspace = hdf5::dataspace::Scalar();
  auto TempDataset = hdf5::node::Group(Node).create_dataset(Name, string_type,
                                                            string_dataspace);
  TempDataset.write(Value, string_type, string_dataspace);
}

} // namespace MetaData
