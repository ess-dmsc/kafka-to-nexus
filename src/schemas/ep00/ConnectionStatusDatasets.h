// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "NeXusDataset.h"

namespace NeXusDataset {

class ConnectionStatusTime : public ExtensibleDataset<std::uint64_t> {
public:
  ConnectionStatusTime() = default;
  /// \brief Create the alarm_time dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  ConnectionStatusTime(hdf5::node::Group const &Parent, Mode CMode,
                       size_t ChunkSize = 1024)
      : ExtensibleDataset<std::uint64_t>(Parent, "time", CMode, ChunkSize) {
    if (Mode::Create == CMode) {
      auto StartAttr =
          ExtensibleDataset::attributes.create<std::string>("start");
      StartAttr.write("1970-01-01T00:00:00Z");
      auto UnitAttr =
          ExtensibleDataset::attributes.create<std::string>("units");
      UnitAttr.write("ns");
    };
  };
};

class ConnectionStatus : public FixedSizeString {
public:
  ConnectionStatus() = default;
  /// \brief Create the alarm_status dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  ConnectionStatus(hdf5::node::Group const &Parent, Mode CMode,
                   size_t ChunkSize = 1024)
      : FixedSizeString(Parent, "connection_status", CMode, ChunkSize){};
};
}
