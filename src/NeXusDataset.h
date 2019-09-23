// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
/// \brief Classes representing NeXus datasets.

#pragma once

#include "ExtensibleDataset.h"

namespace NeXusDataset {


class RawValue : public ExtensibleDataset<std::uint16_t> {
public:
  RawValue() = default;
  /// \brief Create the raw_value dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  RawValue(hdf5::node::Group const &Parent, Mode CMode,
           size_t ChunkSize = 1024);
};

class Time : public ExtensibleDataset<std::uint64_t> {
public:
  Time() = default;
  /// \brief Create the time dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  Time(hdf5::node::Group const &Parent, Mode CMode, size_t ChunkSize = 1024);
};

class CueIndex : public ExtensibleDataset<std::uint32_t> {
public:
  CueIndex() = default;
  /// \brief Create the cue_index dataset of NXLog and NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  CueIndex(hdf5::node::Group const &Parent, Mode CMode,
           size_t ChunkSize = 1024);
};

class CueTimestampZero : public ExtensibleDataset<std::uint64_t> {
public:
  CueTimestampZero() = default;
  /// \brief Create the cue_timestamp_zero dataset of NXLog and NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  CueTimestampZero(hdf5::node::Group const &Parent, Mode CMode,
                   size_t ChunkSize = 1024);
};

class EventId : public ExtensibleDataset<std::uint32_t> {
public:
  EventId() = default;
  /// \brief Create the event_id dataset of NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  EventId(hdf5::node::Group const &Parent, Mode CMode, size_t ChunkSize = 1024);
};

class EventTimeOffset : public ExtensibleDataset<std::uint32_t> {
public:
  EventTimeOffset() = default;
  /// \brief Create the event_time_offset dataset of NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  EventTimeOffset(hdf5::node::Group const &Parent, Mode CMode,
                  size_t ChunkSize = 1024);
};

class EventIndex : public ExtensibleDataset<std::uint32_t> {
public:
  EventIndex() = default;
  /// \brief Create the event_index dataset of NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  EventIndex(hdf5::node::Group const &Parent, Mode CMode,
             size_t ChunkSize = 1024);
};

class EventTimeZero : public ExtensibleDataset<std::uint64_t> {
public:
  EventTimeZero() = default;
  /// \brief Create the event_time_zero dataset of NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  EventTimeZero(hdf5::node::Group const &Parent, Mode CMode,
                size_t ChunkSize = 1024);
};

} // namespace NeXusDataset
