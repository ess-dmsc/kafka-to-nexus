// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2019 European Spallation Source ERIC */

#include "ExtensibleDataset.h"

namespace NeXusDataset {
FixedSizeString::FixedSizeString(const hdf5::node::Group &Parent,
                                 std::string const &Name, Mode CMode,
                                 size_t StringSize, size_t ChunkSize)
    : StringType(hdf5::datatype::String::fixed(StringSize)),
      MaxStringSize(StringSize) {
  StringType.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  StringType.padding(hdf5::datatype::StringPad::NullTerm);
  if (Mode::Create == CMode) {
    dataset_ = hdf5::node::ChunkedDataset(
        Parent, Name, StringType,
        hdf5::dataspace::Simple({0}, {hdf5::dataspace::Simple::unlimited}),
        {
            static_cast<unsigned long long>(ChunkSize),
        });
  } else if (Mode::Open == CMode) {
    dataset_ = Parent.get_dataset(Name);
    hdf5::datatype::String Type(dataset_.datatype());
    MaxStringSize = Type.size();
    NrOfStrings = static_cast<size_t>(dataset_.dataspace().size());
  } else {
    throw std::runtime_error(
        "FixedSizeStringValue::FixedSizeStringValue(): Unknown mode.");
  }
}

void FixedSizeString::appendStringElement(std::string const &InString) {
  dataset_.extent(0, 1);
  hdf5::dataspace::Hyperslab Selection{{NrOfStrings}, {1}};
  hdf5::dataspace::Scalar ScalarSpace;
  hdf5::dataspace::Dataspace FileSpace = dataset_.dataspace();
  FileSpace.selection(hdf5::dataspace::SelectionOperation::Set, Selection);
  dataset_.write(InString, StringType, ScalarSpace, FileSpace);
  NrOfStrings += 1;
}

} // namespace NeXusDataset