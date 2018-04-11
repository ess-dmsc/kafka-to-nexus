#pragma once

#include <h5cpp/hdf5.hpp>

namespace NeXusDataset {

class RawValue : public hdf5::node::ChunkedDataset {
public:
  RawValue() = default;
  /// @brief Create the raw_value dataset of NXLog.
  /// @throw std::runtime_error if dataset already exists.
  RawValue(hdf5::node::Group parent, int ChunkSize);
  /// @brief Open an existing raw_value dataset.
  /// @throw std::runtime_error if dataset does not exist.
  RawValue(hdf5::node::Group parent);
};
  
  class Time : public hdf5::node::ChunkedDataset {
  public:
    Time() = default;
    /// @brief Create the time dataset of NXLog.
    /// @throw std::runtime_error if dataset already exists.
    Time(hdf5::node::Group parent, int ChunkSize);
    /// @brief Open an existing time dataset.
    /// @throw std::runtime_error if dataset does not exist.
    Time(hdf5::node::Group parent);
  };
  
  class CueIndex : public hdf5::node::ChunkedDataset {
  public:
    CueIndex() = default;
    /// @brief Create the cue_index dataset of NXLog.
    /// @throw std::runtime_error if dataset already exists.
    CueIndex(hdf5::node::Group parent, int ChunkSize);
    /// @brief Open an existing cue_index dataset.
    /// @throw std::runtime_error if dataset does not exist.
    CueIndex(hdf5::node::Group parent);
  };
  
  class CueTimestampZero : public hdf5::node::ChunkedDataset {
  public:
    CueTimestampZero() = default;
    /// @brief Create the cue_timestamp_zero dataset of NXLog.
    /// @throw std::runtime_error if dataset already exists.
    CueTimestampZero(hdf5::node::Group parent, int ChunkSize);
    /// @brief Open an existing cue_timestamp_zero dataset.
    /// @throw std::runtime_error if dataset does not exist.
    CueTimestampZero(hdf5::node::Group parent);
    /// @brief Enough characters to fit a ISO8601 data time
    static const int MaxStringLength{30};
  };
  
} // namespace NexUsDataset
