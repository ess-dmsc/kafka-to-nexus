/** Copyright (C) 2018 European Spallation Source ERIC */

/** \file
 *
 *  \brief Define datasets used by the ADC file writing module.
 */

#pragma once

#include "logger.h"
#include <h5cpp/dataspace/simple.hpp>
#include <h5cpp/hdf5.hpp>

/// \todo The following helper classes are used to interface with the h5cpp
/// library. They must be removed from here when they are added to the h5cpp
/// library.

/// \brief Used to write c-arrays to hdf5 files using h5cpp.
///
/// The member functions of this class need no extra documentation.
template <typename T> class ArrayAdapter {
public:
  ArrayAdapter(T *data, size_t size) : data_(data), size_(size) {}
  size_t size() const { return size_; }
  T *data() { return data_; }
  const T *data() const { return data_; }

private:
  T *data_;
  size_t size_;
};

namespace hdf5 {
namespace datatype {
// \brief Required for h5cpp to write const unsigned short data.
template <> class TypeTrait<std::int8_t const> {
public:
  using Type = std::int8_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_INT8)));
  }
};

template <> class TypeTrait<std::uint8_t const> {
public:
  using Type = std::uint8_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_UINT8)));
  }
};

template <> class TypeTrait<std::int16_t const> {
public:
  using Type = std::int16_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_INT16)));
  }
};

template <> class TypeTrait<std::uint16_t const> {
public:
  using Type = std::uint16_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_UINT16)));
  }
};

template <> class TypeTrait<std::int32_t const> {
public:
  using Type = std::int32_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_INT32)));
  }
};

template <> class TypeTrait<std::uint32_t const> {
public:
  using Type = std::uint32_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_UINT32)));
  }
};

template <> class TypeTrait<float const> {
public:
  using Type = float;
  using TypeClass = Float;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_FLOAT)));
  }
};

template <> class TypeTrait<double const> {
public:
  using Type = double;
  using TypeClass = Float;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_DOUBLE)));
  }
};

template <> class TypeTrait<char const> {
public:
  using Type = char;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_CHAR)));
  }
};

template <> class TypeTrait<std::int64_t const> {
public:
  using Type = std::int64_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_INT64)));
  }
};

template <> class TypeTrait<std::uint64_t const> {
public:
  using Type = std::uint64_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return TypeClass(ObjectHandle(H5Tcopy(H5T_NATIVE_UINT64)));
  }
};

// \brief Required for h5cpp to write data provided using ArrayAdapter.
template <typename T> class TypeTrait<ArrayAdapter<T>> {
public:
  using Type = ArrayAdapter<T>;
  using TypeClass = typename TypeTrait<T>::TypeClass;
  static TypeClass create(const Type & = Type()) {
    return TypeTrait<T>::create();
  }
};
} // namespace datatype
namespace dataspace {

// \brief Required for h5cpp to write data provided using ArrayAdapter.
template <typename T> class TypeTrait<ArrayAdapter<T>> {
public:
  using DataspaceType = Simple;

  static DataspaceType create(const ArrayAdapter<T> &value) {
    return Simple(hdf5::Dimensions{value.size()},
                  hdf5::Dimensions{value.size()});
  }

  static void *ptr(ArrayAdapter<T> &data) {
    return reinterpret_cast<void *>(data.data());
  }

  static const void *cptr(const ArrayAdapter<T> &data) {
    return reinterpret_cast<const void *>(data.data());
  }
};
} // namspace dataspace
} // namespace hdf5

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

class MultiDimDatasetBase : public hdf5::node::ChunkedDataset {
public:
  MultiDimDatasetBase() = default;

  /// \brief Open a dataset.
  ///
  /// Can only be used to open a dataset.
  /// \param[in] Parent The group/node of the dataset in.
  /// \note This parameter is ignored when opening an existing dataset.
  /// \param[in] CMode Should the dataset be opened or created.
  /// \throw std::runtime_error if dataset can not opened or the constructor is
  /// called with the input NeXusDataset::Mode::Create.
  MultiDimDatasetBase(hdf5::node::Group Parent, Mode CMode)
      : hdf5::node::ChunkedDataset() {
    if (Mode::Create == CMode) {
      throw std::runtime_error("MultiDimDatasetBase::MultiDimDatasetBase(): "
                               "Can only open datasets, not create.");
    } else if (Mode::Open == CMode) {
      Dataset::operator=(Parent.get_dataset("value"));
    } else {
      throw std::runtime_error(
          "MultiDimDatasetBase::MultiDimDatasetBase(): Unknown mode.");
    }
  }

  hdf5::Dimensions get_extent() {
    auto DataSpace = dataspace();
    return hdf5::dataspace::Simple(DataSpace).current_dimensions();
  }

  /// \brief Append data to dataset that is contained in some sort of container.
  template <typename T>
  void appendArray(T const &NewData, hdf5::Dimensions Shape) {
    auto CurrentExtent = get_extent();
    hdf5::Dimensions Origin(CurrentExtent.size(), 0);
    Origin[0] = CurrentExtent[0];
    ++CurrentExtent[0];
    Shape.insert(Shape.begin(), 1);
    if (Shape.size() != CurrentExtent.size()) {
      LOG(Sev::Error,
          "Data has {} dimension(s) and dataset has {} (+1) dimensions.",
          Shape.size() - 1, CurrentExtent.size() - 1);
      throw std::runtime_error(
          "Rank (dimensions) of data to be written is wrong.");
    }
    for (size_t i = 1; i < Shape.size(); i++) {
      if (Shape[i] > CurrentExtent[i]) {
        LOG(Sev::Warning, "Dimension {} of new data is larger than that of the "
                          "dataset. Extending dataset.",
            i - 1);
        CurrentExtent[i] = Shape[i];
      } else if (Shape[i] < CurrentExtent[i]) {
        LOG(Sev::Warning, "Dimension {} of new data is smaller than that of "
                          "the dataset. Using 0 as a filler.",
            i - 1);
      }
    }
    Dataset::extent(CurrentExtent);
    hdf5::dataspace::Hyperslab Selection{{Origin}, {Shape}};
    write(NewData, Selection);
  }
};

/// \brief h5cpp dataset class that implements methods for appending data.
template <class DataType> class MultiDimDataset : public MultiDimDatasetBase {
public:
  MultiDimDataset() = default;
  /// \brief Will create or open dataset with the given name.
  ///
  /// When opening a dataset, some of the paramaters will be ignored.
  /// \param[in] Parent The group/node of the dataset in.
  /// \note This parameter is ignored when opening an existing dataset.
  /// \param[in] CMode Should the dataset be opened or created.
  /// \param[in] Shape The shape of the array in the NDArray. This vector will
  /// be prepended with one dimension to allow for adding of data.
  /// \param[in] ChunkSize The hunk size of the dataset, ignored if the dataset
  /// is opened.
  /// \throw std::runtime_error if dataset can not be created/opened.
  MultiDimDataset(hdf5::node::Group Parent, Mode CMode, hdf5::Dimensions Shape,
                  hdf5::Dimensions ChunkSize)
      : MultiDimDatasetBase() {
    if (Mode::Create == CMode) {
      Shape.insert(Shape.begin(), 0);
      hdf5::Dimensions MaxSize(Shape.size(),
                               int(hdf5::dataspace::Simple::UNLIMITED));
      std::vector<hsize_t> VectorChunkSize;
      if (ChunkSize.empty()) {
        LOG(Sev::Warning, "No chunk size given. Using the default value 1024.");
        ChunkSize.emplace_back(1024);
      }
      if (ChunkSize.size() == Shape.size()) {
        VectorChunkSize = ChunkSize;
      } else if (ChunkSize.size() == 1 and Shape.size() > 1) {
        VectorChunkSize = Shape;
        VectorChunkSize[0] = ChunkSize[0];
      } else {
        LOG(Sev::Error, "Unable to reconcile a data shape with {} dimensions "
                        "and chunk size with {} dimensions. Using default "
                        "values.",
            Shape.size(), ChunkSize.size());
        VectorChunkSize = Shape;
        VectorChunkSize[0] = 1024;
      }
      Dataset::operator=(hdf5::node::ChunkedDataset(
          Parent, "value", hdf5::datatype::create<DataType>(),
          hdf5::dataspace::Simple(Shape, MaxSize), VectorChunkSize));
    } else if (Mode::Open == CMode) {
      Dataset::operator=(Parent.get_dataset("value"));
    } else {
      throw std::runtime_error(
          "MultiDimDataset::MultiDimDataset(): Unknown mode.");
    }
  }

  /// \brief Open a dataset.
  ///
  /// Can only be used to open a dataset.
  /// \param[in] Parent The group/node of the dataset in.
  /// \note This parameter is ignored when opening an existing dataset.
  /// \param[in] CMode Should the dataset be opened or created.
  /// \throw std::runtime_error if dataset can not opened or the constructor is
  /// called with the input NeXusDataset::Mode::Create.
  MultiDimDataset(hdf5::node::Group Parent, Mode CMode)
      : MultiDimDatasetBase(Parent, CMode) {}
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
