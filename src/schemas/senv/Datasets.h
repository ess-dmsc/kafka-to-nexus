/** Copyright (C) 2018 European Spallation Source ERIC */

/** \file
 *
 *  \brief Define datasets used by the ADC file writing module.
 */

#pragma once

#include <h5cpp/hdf5.hpp>

namespace NeXusDataset {
enum class Mode { Create, Open };
/// \brief h5cpp dataset class that implements methods for appending data.
template <class DataType>
class ExtensibleDataset : public hdf5::node::ChunkedDataset {
public:
  ExtensibleDataset() = default;
  /// \brief Will create or open dataset with the given name.
  /// \param[in] Parent The group/node of the dataset in.
  /// \param[in] Name The name of the dataset.
  /// \param[in] CMode Should the dataset be opened or created.
  /// \param[in] ChunkSize The hunk size of the dataset, ignored if the dataset
  /// is opened.
  /// \throw std::runtime_error if dataset can not be created/opened.
  ExtensibleDataset(hdf5::node::Group Parent, std::string Name, Mode CMode,
                    int ChunkSize)
      : hdf5::node::ChunkedDataset() {
    if (Mode::Create == CMode) {
      Dataset::operator=(hdf5::node::ChunkedDataset(
          Parent, Name, hdf5::datatype::create<DataType>(),
          hdf5::dataspace::Simple({0}, {hdf5::dataspace::Simple::UNLIMITED}),
          {
              static_cast<unsigned long long>(ChunkSize),
          }));
    } else if (Mode::Open == CMode) {
      Dataset::operator=(Parent.get_dataset(Name));
      NrOfElements = dataspace().size();
    } else {
      throw std::runtime_error(
          "ExtensibleDataset::ExtensibleDataset(): Unknown mode.");
    }
  }

  /// \brief Append data to dataset that is contained in some sort of container.
  template <typename T> void appendArray(T const &NewData) {
    Dataset::extent(0,
                    NewData.size()); // Extend size() element along dimenions 0
    hdf5::dataspace::Hyperslab Selection{
        {NrOfElements}, {static_cast<unsigned long long>(NewData.size())}};
    write(NewData, Selection);
    NrOfElements += NewData.size();
  }

  /// \brief Append single scalar values to dataset.
  template <typename T> void appendElement(T const &NewElement) {
    Dataset::extent(0, 1); // Extend by 1 element along dimenions 0
    hdf5::dataspace::Hyperslab Selection{{NrOfElements}, {1}};
    write(NewElement, Selection);
    NrOfElements += 1;
  }

private:
  size_t NrOfElements{0};
};

// Make all of single param constructors explicit
class RawValue : public ExtensibleDataset<std::uint16_t> {
public:
  RawValue() = default;
  /// \brief Create the raw_value dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  RawValue(hdf5::node::Group Parent, Mode CMode, int ChunkSize = 1024);
};

class Time : public ExtensibleDataset<std::uint64_t> {
public:
  Time() = default;
  /// \brief Create the time dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  Time(hdf5::node::Group parent, Mode CMode, int ChunkSize = 1024);
};

class CueIndex : public ExtensibleDataset<std::uint32_t> {
public:
  CueIndex() = default;
  /// \brief Create the cue_index dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  CueIndex(hdf5::node::Group parent, Mode CMode, int ChunkSize = 1024);
};

class CueTimestampZero : public ExtensibleDataset<std::uint64_t> {
public:
  CueTimestampZero() = default;
  /// \brief Create the cue_timestamp_zero dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  CueTimestampZero(hdf5::node::Group parent, Mode CMode, int ChunkSize = 1024);
};

} // namespace NexUsDataset
