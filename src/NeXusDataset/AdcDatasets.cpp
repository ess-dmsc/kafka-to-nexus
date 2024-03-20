#include "AdcDatasets.h"

namespace NeXusDataset {

ThresholdTime::ThresholdTime(hdf5::node::Group const &Parent, Mode CMode,
                             size_t ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "adc_pulse_threshold_time",
                                       CMode, ChunkSize) {
  if (CMode == Mode::Create) {
    auto StartAttr = dataset_.attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = dataset_.attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
}

PeakTime::PeakTime(hdf5::node::Group const &Parent, Mode CMode,
                   size_t ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "adc_pulse_peak_time", CMode,
                                       ChunkSize) {
  if (CMode == Mode::Create) {
    auto StartAttr = dataset_.attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = dataset_.attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
}

} // namespace NeXusDataset
