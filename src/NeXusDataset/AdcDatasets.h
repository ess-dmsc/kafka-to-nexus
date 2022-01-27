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

/// \brief Represents a NeXus dataset with the name "adc_pulse_amplitude".
class Amplitude : public ExtensibleDataset<std::uint32_t> {
public:
  Amplitude() = default;
  Amplitude(hdf5::node::Group const &Parent, Mode CMode,
            size_t ChunkSize = 1024)
      : ExtensibleDataset<std::uint32_t>(Parent, "adc_pulse_amplitude", CMode,
                                         ChunkSize){};
};

/// \brief Represents a NeXus dataset with the name "adc_pulse_peak_area".
class PeakArea : public ExtensibleDataset<std::uint32_t> {
public:
  PeakArea() = default;
  PeakArea(hdf5::node::Group const &Parent, Mode CMode, size_t ChunkSize = 1024)
      : ExtensibleDataset<std::uint32_t>(Parent, "adc_pulse_peak_area", CMode,
                                         ChunkSize){};
};

/// \brief Represents a NeXus dataset with the name "adc_pulse_background".
class Background : public ExtensibleDataset<std::uint32_t> {
public:
  Background() = default;
  Background(hdf5::node::Group const &Parent, Mode CMode,
             size_t ChunkSize = 1024)
      : ExtensibleDataset<std::uint32_t>(Parent, "adc_pulse_background", CMode,
                                         ChunkSize){};
};

/// \brief Represents a NeXus dataset with the name "adc_pulse_threshold_time".
class ThresholdTime : public ExtensibleDataset<std::uint64_t> {
public:
  ThresholdTime() = default;
  ThresholdTime(hdf5::node::Group const &Parent, Mode CMode,
                size_t ChunkSize = 1024);
};

/// \brief Represents a NeXus dataset with the name "adc_pulse_peak_time".
class PeakTime : public ExtensibleDataset<std::uint64_t> {
public:
  PeakTime() = default;
  PeakTime(hdf5::node::Group const &Parent, Mode CMode,
           size_t ChunkSize = 1024);
};

} // namespace NeXusDataset
