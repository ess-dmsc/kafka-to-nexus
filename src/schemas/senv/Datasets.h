#pragma once

#include <h5cpp/hdf5.hpp>

namespace NeXusDataset {

  template <class DataType>
  class ExtensibleDataset : public hdf5::node::ChunkedDataset {
  public:
    ExtensibleDataset() = default;
    ExtensibleDataset(hdf5::node::Group Parent, std::string Name, int ChunkSize) : hdf5::node::ChunkedDataset(Parent, Name, hdf5::datatype::create<DataType>(), hdf5::dataspace::Simple({0}, {hdf5::dataspace::Simple::UNLIMITED}), {static_cast<unsigned long long>(ChunkSize), }) {}
    
    ExtensibleDataset(hdf5::node::Group Parent, std::string Name) {
      Dataset::operator=(Parent.get_dataset(Name));
      NrOfElements = dataspace().size();
    }
    
    template <typename T>
    void appendData(T const &NewData) {
      Dataset::extent(0, NewData.size()); // Extend size() element along dimenions 0
      hdf5::dataspace::Hyperslab Selection{{NrOfElements}, {static_cast<unsigned long long>(NewData.size())}};
      write(NewData, Selection);
      NrOfElements += NewData.size();
    }
  private:
    size_t NrOfElements{0};
  };

  class RawValue : public ExtensibleDataset<std::uint16_t> {
public:
  RawValue() = default;
  /// @brief Create the raw_value dataset of NXLog.
  /// @throw std::runtime_error if dataset already exists.
  RawValue(hdf5::node::Group Parent, int ChunkSize);
  /// @brief Open an existing raw_value dataset.
  /// @throw std::runtime_error if dataset does not exist.
  RawValue(hdf5::node::Group parent);
};
  
  class Time : public ExtensibleDataset<std::uint64_t> {
  public:
    Time() = default;
    /// @brief Create the time dataset of NXLog.
    /// @throw std::runtime_error if dataset already exists.
    Time(hdf5::node::Group parent, int ChunkSize);
    /// @brief Open an existing time dataset.
    /// @throw std::runtime_error if dataset does not exist.
    Time(hdf5::node::Group parent);
  };
  
  class CueIndex : public ExtensibleDataset<std::uint32_t> {
  public:
    CueIndex() = default;
    /// @brief Create the cue_index dataset of NXLog.
    /// @throw std::runtime_error if dataset already exists.
    CueIndex(hdf5::node::Group parent, int ChunkSize);
    /// @brief Open an existing cue_index dataset.
    /// @throw std::runtime_error if dataset does not exist.
    CueIndex(hdf5::node::Group parent);
  };
  
  class CueTimestampZero : public ExtensibleDataset<std::uint64_t> {
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
