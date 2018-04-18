#include "Datasets.h"

namespace NeXusDataset {
RawValue::RawValue(hdf5::node::Group parent, int ChunkSize)
    : ExtensibleDataset<std::uint16_t>(parent, "raw_value", ChunkSize) {}

RawValue::RawValue(hdf5::node::Group parent)
    : ExtensibleDataset<std::uint16_t>() {
  Dataset::operator=(parent.get_dataset("raw_value"));
}

Time::Time(hdf5::node::Group parent, int ChunkSize)
    : ExtensibleDataset<std::uint64_t>(parent, "time", ChunkSize) {}

Time::Time(hdf5::node::Group parent) : ExtensibleDataset<std::uint64_t>() {
  Dataset::operator=(parent.get_dataset("time"));
}

CueIndex::CueIndex(hdf5::node::Group parent, int ChunkSize)
    : ExtensibleDataset<std::uint32_t>(parent, "cue_index", ChunkSize) {}

CueIndex::CueIndex(hdf5::node::Group parent)
    : ExtensibleDataset<std::uint32_t>() {
  Dataset::operator=(parent.get_dataset("cue_index"));
}

CueTimestampZero::CueTimestampZero(hdf5::node::Group parent, int ChunkSize)
    : ExtensibleDataset<std::uint64_t>(parent, "cue_timestamp_zero",
                                       ChunkSize) {}

CueTimestampZero::CueTimestampZero(hdf5::node::Group parent)
    : ExtensibleDataset<std::uint64_t>() {
  Dataset::operator=(parent.get_dataset("cue_timestamp_zero"));
}

} // namespace NeXusDataset
