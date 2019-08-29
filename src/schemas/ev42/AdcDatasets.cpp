#include "AdcDatasets.h"

namespace NeXusDataset {

ThresholdTime::ThresholdTime(hdf5::node::Group const &Parent, Mode CMode,
                             size_t ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "threshold_time", CMode,
                                       ChunkSize) {
  if (CMode == Mode::Create) {
    auto StartAttr = ExtensibleDataset::attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = ExtensibleDataset::attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
};

PeakTime::PeakTime(hdf5::node::Group const &Parent, Mode CMode,
                   size_t ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "peak_time", CMode, ChunkSize) {
  if (CMode == Mode::Create) {
    auto StartAttr = ExtensibleDataset::attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = ExtensibleDataset::attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
};

} // NeXusDataset namespace
