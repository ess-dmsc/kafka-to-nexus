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
UInt16Value::UInt16Value(hdf5::node::Group const &Parent, Mode CMode,
                         size_t ChunkSize)
    : ExtensibleDataset<std::uint16_t>(Parent, "value", CMode, ChunkSize) {}

Time::Time(hdf5::node::Group const &Parent, Mode CMode, size_t ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "time", CMode, ChunkSize) {
  if (Mode::Create == CMode) {
    auto StartAttr = ExtensibleDataset::attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = ExtensibleDataset::attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
}

DateTime::DateTime(hdf5::node::Group const &Parent, std::string const &name,
                   Mode CMode, size_t ChunkSize)
    : ExtensibleDataset<const char[32]>(Parent, name.c_str(), CMode,
                                        ChunkSize) {}

DoubleValue::DoubleValue(hdf5::node::Group const &Parent,
                         NeXusDataset::Mode CMode, size_t ChunkSize)
    : NeXusDataset::ExtensibleDataset<double>(Parent, "value", CMode,
                                              ChunkSize) {}

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
                                       ChunkSize) {
  if (Mode::Create == CMode) {
    auto UnitAttr = ExtensibleDataset::attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
}

EventTimeZeroIndex::EventTimeZeroIndex(hdf5::node::Group const &Parent,
                                       Mode CMode, size_t ChunkSize)
    : ExtensibleDataset<std::uint32_t>(Parent, "event_time_zero_index", CMode,
                                       ChunkSize) {}

EventIndex::EventIndex(hdf5::node::Group const &Parent, Mode CMode,
                       size_t ChunkSize)
    : ExtensibleDataset<std::uint32_t>(Parent, "event_index", CMode,
                                       ChunkSize) {}

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

} // namespace NeXusDataset
