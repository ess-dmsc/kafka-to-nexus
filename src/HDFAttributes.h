// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "MultiVector.h"
#include "TimeUtility.h"
#include <h5cpp/hdf5.hpp>

namespace HDFAttributes {

inline void writeAttribute(hdf5::node::Node const &Node,
                           const std::string &Name, std::string const &Value) {
  auto string_type = hdf5::datatype::String::variable();
  string_type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  string_type.padding(hdf5::datatype::StringPad::NULLTERM);
  Node.attributes.create(Name, string_type, hdf5::dataspace::Scalar())
      .write(Value, string_type);
}

template <typename T>
void writeAttribute(hdf5::node::Node const &Node, const std::string &Name,
                    T Value) {
  Node.attributes.create<T>(Name).write(Value);
}

template <typename T>
void writeAttribute(hdf5::node::Node const &Node, const std::string &Name,
                    std::vector<T> Values) {
  Node.attributes.create<T>(Name, {Values.size()}).write(Values);
}

inline void writeAttribute(hdf5::node::Node const &Node,
                           const std::string &Name,
                           MultiVector<std::string> Values) {
  auto string_type = hdf5::datatype::String::variable();
  string_type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  string_type.padding(hdf5::datatype::StringPad::NULLTERM);
  auto Dims = Values.getDimensions();
  Node.attributes
      .create(
          Name, string_type,
          hdf5::dataspace::Simple(hdf5::Dimensions(Dims.begin(), Dims.end())))
      .write(Values);
}

template <typename T>
void writeAttribute(hdf5::node::Node const &Node, const std::string &Name,
                    MultiVector<T> Values) {
  auto Dims = Values.getDimensions();
  Node.attributes.create<T>(Name, hdf5::Dimensions(Dims.begin(), Dims.end()))
      .write(Values);
}

void writeAttribute(hdf5::node::Node const &Node, const std::string &Name,
                    time_point Value);

} // namespace HDFAttributes
