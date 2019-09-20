// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2018 European Spallation Source ERIC */

#include "NeXusDataset.h"

namespace NeXusDataset {
RawValue::RawValue(hdf5::node::Group const &Parent, Mode CMode,
                   size_t ChunkSize)
    : ExtensibleDataset<std::uint16_t>(Parent, "raw_value", CMode, ChunkSize) {}

Time::Time(hdf5::node::Group const &Parent, Mode CMode, size_t ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "time", CMode, ChunkSize) {
  if (Mode::Create == CMode) {
    auto StartAttr = ExtensibleDataset::attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = ExtensibleDataset::attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
}

CueIndex::CueIndex(hdf5::node::Group const &Parent, Mode CMode,
                   size_t ChunkSize)
    : ExtensibleDataset<std::uint32_t>(Parent, "cue_index", CMode, ChunkSize) {}

CueTimestampZero::CueTimestampZero(hdf5::node::Group const &Parent, Mode CMode,
                                   size_t ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "cue_timestamp_zero", CMode,
                                       ChunkSize) {
  if (Mode::Create == CMode) {
    auto StartAttr = ExtensibleDataset::attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = ExtensibleDataset::attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
}

EventId::EventId(hdf5::node::Group const &Parent, Mode CMode, size_t ChunkSize)
    : ExtensibleDataset<std::uint32_t>(Parent, "event_id", CMode, ChunkSize) {}

EventTimeOffset::EventTimeOffset(hdf5::node::Group const &Parent, Mode CMode,
                                 size_t ChunkSize)
    : ExtensibleDataset<std::uint32_t>(Parent, "event_time_offset", CMode,
                                       ChunkSize) {}

EventIndex::EventIndex(hdf5::node::Group const &Parent, Mode CMode,
                       size_t ChunkSize)
    : ExtensibleDataset<std::uint32_t>(Parent, "EventIndex", CMode, ChunkSize) {
}

EventTimeZero::EventTimeZero(hdf5::node::Group const &Parent, Mode CMode,
                             size_t ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "event_time_zero", CMode,
                                       ChunkSize) {
  if (Mode::Create == CMode) {
    auto StartAttr = ExtensibleDataset::attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = ExtensibleDataset::attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
}

FixedSizeString::FixedSizeString(const hdf5::node::Group &Parent,
                                 std::string Name, Mode CMode,
                                 size_t StringSize, size_t ChunkSize)
    : hdf5::node::ChunkedDataset(),
      StringType(hdf5::datatype::String::fixed(StringSize)),
      MaxStringSize(StringSize) {
  StringType.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  StringType.padding(hdf5::datatype::StringPad::NULLTERM);
  if (Mode::Create == CMode) {
    Dataset::operator=(hdf5::node::ChunkedDataset(
        Parent, Name, StringType,
        hdf5::dataspace::Simple({0}, {hdf5::dataspace::Simple::UNLIMITED}),
        {
            static_cast<unsigned long long>(ChunkSize),
        }));
  } else if (Mode::Open == CMode) {
    Dataset::operator=(Parent.get_dataset(Name));
    hdf5::datatype::String Type(datatype());
    MaxStringSize = Type.size();
    NrOfStrings = static_cast<size_t>(dataspace().size());
  } else {
    throw std::runtime_error(
        "FixedSizeStringValue::FixedSizeStringValue(): Unknown mode.");
  }
}
size_t FixedSizeString::getMaxStringSize() const { return MaxStringSize; }
void FixedSizeString::appendString(std::string const &InString) {
  Dataset::extent(0, 1);
  hdf5::dataspace::Hyperslab Selection{{NrOfStrings}, {1}};
  hdf5::dataspace::Scalar scalar_space;
  hdf5::dataspace::Dataspace FileSpace = dataspace();
  FileSpace.selection(hdf5::dataspace::SelectionOperation::SET, Selection);
  write(InString, StringType, scalar_space,
        FileSpace); //, scalar_space, Selection,
                    //hdf5::property::DatasetTransferList());
  NrOfStrings += 1;
}

} // namespace NeXusDataset
