// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "EpicsAlarmDatasets.h"

namespace NeXusDataset {

AlarmTime::AlarmTime(hdf5::node::Group const &Parent, Mode CMode,
                     size_t ChunkSize)
    : ExtensibleDataset<std::uint64_t>(Parent, "alarm_time", CMode, ChunkSize) {
  if (Mode::Create == CMode) {
    auto StartAttr = dataset_.attributes.create<std::string>("start");
    StartAttr.write("1970-01-01T00:00:00Z");
    auto UnitAttr = dataset_.attributes.create<std::string>("units");
    UnitAttr.write("ns");
  }
}

} // namespace NeXusDataset
