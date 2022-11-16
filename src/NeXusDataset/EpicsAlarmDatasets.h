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

/// \brief Represents a dataset with the name "alarm_status".
class AlarmStatus : public FixedSizeString {
public:
  AlarmStatus() = default;
  AlarmStatus(hdf5::node::Group const &Parent, Mode CMode,
              size_t StringSize = 32, size_t ChunkSize = 1024)
      : FixedSizeString(Parent, "alarm_status", CMode, StringSize, ChunkSize){};
};

/// \brief Represents a dataset with the name "alarm_message".
class AlarmMsg : public FixedSizeString {
public:
  AlarmMsg() = default;
  AlarmMsg(hdf5::node::Group const &Parent, Mode CMode,
                size_t StringSize = 200, size_t ChunkSize = 1024)
      : FixedSizeString(Parent, "alarm_message", CMode, StringSize,
                        ChunkSize){};
};

/// \brief Represents a dataset with the name "alarm_severity".
class AlarmSeverity : public FixedSizeString {
public:
  AlarmSeverity() = default;
  AlarmSeverity(hdf5::node::Group const &Parent, Mode CMode,
                size_t StringSize = 32, size_t ChunkSize = 1024)
      : FixedSizeString(Parent, "alarm_severity", CMode, StringSize,
                        ChunkSize){};
};

/// \brief Represents a timestamp dataset.
class AlarmTime : public ExtensibleDataset<std::uint64_t> {
public:
  AlarmTime() = default;
  AlarmTime(hdf5::node::Group const &Parent, Mode CMode,
            size_t ChunkSize = 1024);
};

} // namespace NeXusDataset
