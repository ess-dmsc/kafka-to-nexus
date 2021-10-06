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

/// \brief Class for representing a uint16 NeXus dataset.
class UInt16Value : public ExtensibleDataset<std::uint16_t> {
public:
  UInt16Value() = default;
  /// \brief Create the raw_value dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  UInt16Value(hdf5::node::Group const &Parent, Mode CMode,
              size_t ChunkSize = 1024);
};

/// \brief Class for representing a double precision floating point NeXus
/// dataset.
class DoubleValue : public NeXusDataset::ExtensibleDataset<double> {
public:
  DoubleValue() = default;
  /// \brief Create the value dataset of NXLog.
  DoubleValue(hdf5::node::Group const &Parent, NeXusDataset::Mode CMode,
              size_t ChunkSize = 1024);
};

/// \brief Class for representing a timestamp (NeXus) dataset where the
/// timestamps are in ns since UNIX epoch.
class Time : public ExtensibleDataset<std::uint64_t> {
public:
  Time() = default;
  /// \brief Create the time dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  Time(hdf5::node::Group const &Parent, Mode CMode, size_t ChunkSize = 1024);
};

/// \brief Represents the index register for searching a large NXlog
/// (relatively) quickly based on timestamp.
class CueIndex : public ExtensibleDataset<std::uint32_t> {
public:
  CueIndex() = default;
  /// \brief Create the cue_index dataset of NXLog and NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  CueIndex(hdf5::node::Group const &Parent, Mode CMode,
           size_t ChunkSize = 1024);
};

/// \brief Represents the timestamp register for searching a large NXlog
/// (relatively) quickly.
class CueTimestampZero : public ExtensibleDataset<std::uint64_t> {
public:
  CueTimestampZero() = default;
  /// \brief Create the cue_timestamp_zero dataset of NXLog and NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  CueTimestampZero(hdf5::node::Group const &Parent, Mode CMode,
                   size_t ChunkSize = 1024);
};

/// \brief Represents the (radiation) detector event id dataset in a
/// NXevent_data.
class EventId : public ExtensibleDataset<std::uint32_t> {
public:
  EventId() = default;
  /// \brief Create the event_id dataset of NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  EventId(hdf5::node::Group const &Parent, Mode CMode, size_t ChunkSize = 1024);
};

/// \brief Represents the (radiation) detector event timestamp offset from zero
/// time in a NXevent_data.
class EventTimeOffset : public ExtensibleDataset<std::uint32_t> {
public:
  EventTimeOffset() = default;
  /// \brief Create the event_time_offset dataset of NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  EventTimeOffset(hdf5::node::Group const &Parent, Mode CMode,
                  size_t ChunkSize = 1024);
};

/// \brief Represents the (radiation) detector event index that ties
/// EventTimeZero to event id and offset in a NXevent_data.
class EventIndex : public ExtensibleDataset<std::uint32_t> {
public:
  EventIndex() = default;
  /// \brief Create the event_index dataset of NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  EventIndex(hdf5::node::Group const &Parent, Mode CMode,
             size_t ChunkSize = 1024);
};

/// \brief Represents the (radiation) detector event reference timestamp dataset
/// in a NXevent_data.
class EventTimeZero : public ExtensibleDataset<std::uint64_t> {
public:
  EventTimeZero() = default;
  /// \brief Create the event_time_zero dataset of NXevent_data.
  /// \throw std::runtime_error if dataset already exists.
  EventTimeZero(hdf5::node::Group const &Parent, Mode CMode,
                size_t ChunkSize = 1024);
};

} // namespace NeXusDataset
