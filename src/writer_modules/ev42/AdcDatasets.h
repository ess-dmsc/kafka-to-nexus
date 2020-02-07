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

class Amplitude : public ExtensibleDataset<std::uint32_t> {
public:
  Amplitude() = default;
  /// \brief Create the raw_value dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  Amplitude(hdf5::node::Group const &Parent, Mode CMode,
            size_t ChunkSize = 1024)
      : ExtensibleDataset<std::uint32_t>(Parent, "adc_pulse_amplitude", CMode,
                                         ChunkSize){};
};

class PeakArea : public ExtensibleDataset<std::uint32_t> {
public:
  PeakArea() = default;
  /// \brief Create the raw_value dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  PeakArea(hdf5::node::Group const &Parent, Mode CMode, size_t ChunkSize = 1024)
      : ExtensibleDataset<std::uint32_t>(Parent, "adc_pulse_peak_area", CMode,
                                         ChunkSize){};
};

class Background : public ExtensibleDataset<std::uint32_t> {
public:
  Background() = default;
  /// \brief Create the raw_value dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  Background(hdf5::node::Group const &Parent, Mode CMode,
             size_t ChunkSize = 1024)
      : ExtensibleDataset<std::uint32_t>(Parent, "adc_pulse_background", CMode,
                                         ChunkSize){};
};

class ThresholdTime : public ExtensibleDataset<std::uint64_t> {
public:
  ThresholdTime() = default;
  /// \brief Create the raw_value dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  ThresholdTime(hdf5::node::Group const &Parent, Mode CMode,
                size_t ChunkSize = 1024);
};

class PeakTime : public ExtensibleDataset<std::uint64_t> {
public:
  PeakTime() = default;
  /// \brief Create the raw_value dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  PeakTime(hdf5::node::Group const &Parent, Mode CMode,
           size_t ChunkSize = 1024);
};

} // namespace NeXusDataset
