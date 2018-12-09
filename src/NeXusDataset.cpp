/** Copyright (C) 2018 European Spallation Source ERIC */

#include "NeXusDataset.h"

namespace NeXusDataset {
RawValue::RawValue(hdf5::node::Group Parent, Mode CMode, int ChunkSize)
    : ExtensibleDataset<std::uint16_t>(Parent, "raw_value", CMode, ChunkSize) {}

Time::Time(hdf5::node::Group Parent, Mode CMode, int ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "time", CMode, ChunkSize) {
  if (Mode::Create == CMode) {
    auto StartAttr = ExtensibleDataset::attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = ExtensibleDataset::attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
}

CueIndex::CueIndex(hdf5::node::Group Parent, Mode CMode, int ChunkSize)
    : ExtensibleDataset<std::uint32_t>(Parent, "cue_index", CMode, ChunkSize) {}

CueTimestampZero::CueTimestampZero(hdf5::node::Group Parent, Mode CMode,
                                   int ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "cue_timestamp_zero", CMode,
                                       ChunkSize) {
  if (Mode::Create == CMode) {
    auto StartAttr = ExtensibleDataset::attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = ExtensibleDataset::attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
}
} // namespace NeXusDataset
