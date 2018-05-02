/** Copyright (C) 2018 European Spallation Source ERIC */

/** \file
 *
 *  \brief Implement datasets used by the ADC file writing module.
 */

#include "Datasets.h"

namespace NeXusDataset {
RawValue::RawValue(hdf5::node::Group parent, Mode CMode, int ChunkSize)
    : ExtensibleDataset<std::uint16_t>(parent, "raw_value", CMode, ChunkSize) {}

Time::Time(hdf5::node::Group parent, Mode CMode, int ChunkSize)
    : ExtensibleDataset<std::uint64_t>(parent, "time", CMode, ChunkSize) {}

CueIndex::CueIndex(hdf5::node::Group parent, Mode CMode, int ChunkSize)
    : ExtensibleDataset<std::uint32_t>(parent, "cue_index", CMode, ChunkSize) {}

CueTimestampZero::CueTimestampZero(hdf5::node::Group parent, Mode CMode,
                                   int ChunkSize)
    : ExtensibleDataset<std::uint64_t>(parent, "cue_timestamp_zero", CMode,
                                       ChunkSize) {}
} // namespace NeXusDataset
