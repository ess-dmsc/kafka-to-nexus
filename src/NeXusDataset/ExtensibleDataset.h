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
/// \brief Base classes for NeXus datasets.

#pragma once

#include "../logger.h"
#include <h5cpp/dataspace/simple.hpp>
#include <h5cpp/hdf5.hpp>
#include <h5cpp/utilities/array_adapter.hpp>

namespace hdf5 {
namespace datatype {
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
} // namespace datatype
} // namespace hdf5

namespace NeXusDataset {

enum class Mode { Create, Open };

class ExtensibleDatasetBase : public hdf5::node::ChunkedDataset {
public:
  ExtensibleDatasetBase() = default;
  ExtensibleDatasetBase(const hdf5::node::Group &Parent, std::string Name,
                        Mode CMode)
      : hdf5::node::ChunkedDataset() {
    if (Mode::Create == CMode) {
      throw std::runtime_error(
          "ExtensibleDatasetBase::ExtensibleDatasetBase(): "
          "Can only open datasets, not create.");
    } else if (Mode::Open == CMode) {
      Dataset::operator=(Parent.get_dataset(Name));
    } else {
      throw std::runtime_error(
          "ExtensibleDatasetBase::ExtensibleDatasetBase(): Unknown mode.");
    }
  }

  /// Append data to dataset that is contained in some sort of container.
  template <typename T> void appendArray(T const &NewData) {
    Dataset::extent(0,
                    NewData.size()); // Extend size() element along dimenions 0
    hdf5::dataspace::Hyperslab Selection{
        {NrOfElements}, {static_cast<unsigned long long>(NewData.size())}};
    write(NewData, Selection);
    NrOfElements += NewData.size();
  }

  /// Append single scalar values to dataset.
  template <typename T> void appendElement(T const &NewElement) {
    Dataset::extent(0, 1); // Extend by 1 element along dimenions 0
    hdf5::dataspace::Hyperslab Selection{{NrOfElements}, {1}};
    write(NewElement, Selection);
    NrOfElements += 1;
  }

  template <class DataType>
  void appendArray(hdf5::ArrayAdapter<const DataType> const &NewData) {
    if (NewData.size() == 0) {
      return;
    }
    NewDimensions[0] = NrOfElements + NewData.size();
    Dataset::resize(NewDimensions);
    ArraySelection.offset({NrOfElements});
    ArraySelection.block({static_cast<unsigned long long>(NewData.size())});

    ArrayDataSpace.dimensions({NewData.size()}, {NewData.size()});
    hdf5::dataspace::Dataspace FileSpace = dataspace();
    FileSpace.selection(hdf5::dataspace::SelectionOperation::SET,
                        ArraySelection);
    hdf5::datatype::Datatype ArrayValueType{hdf5::datatype::create(DataType())};
    write(NewData, ArrayValueType, ArrayDataSpace, FileSpace, Dtpl);

    NrOfElements += NewData.size();
  }

protected:
  hdf5::dataspace::Simple ArrayDataSpace;
  hdf5::Dimensions NewDimensions{0};
  hdf5::dataspace::Hyperslab ArraySelection{{0}, {1}};
  hdf5::property::DatasetTransferList Dtpl;
  size_t NrOfElements{0};
};

/// h5cpp dataset class that implements methods for appending data.
template <class DataType>
class ExtensibleDataset : public ExtensibleDatasetBase {
public:
  ExtensibleDataset() = default;
  /// \brief Will create or open dataset with the given name.
  /// \param Parent The group/node of the dataset in.
  /// \param Name The name of the dataset.
  /// \param CMode Should the dataset be opened or created.
  /// \param ChunkSize The hunk size (as number of elements) of the dataset,
  /// ignored if the dataset is opened.
  ExtensibleDataset(hdf5::node::Group const &Parent, std::string Name,
                    Mode CMode, size_t ChunkSize = 1024)
      : ExtensibleDatasetBase() {
    if (Mode::Create == CMode) {
      Dataset::operator=(hdf5::node::ChunkedDataset(
          Parent, Name, hdf5::datatype::create<DataType>(),
          hdf5::dataspace::Simple({0}, {hdf5::dataspace::Simple::UNLIMITED}),
          {
              static_cast<unsigned long long>(ChunkSize),
          }));
    } else if (Mode::Open == CMode) {
      Dataset::operator=(Parent.get_dataset(Name));
      NrOfElements = static_cast<size_t>(dataspace().size());
    } else {
      throw std::runtime_error(
          "ExtensibleDataset::ExtensibleDataset(): Unknown mode.");
    }
  }
};

class FixedSizeString : public hdf5::node::ChunkedDataset {
public:
  FixedSizeString() = default;
  /// \brief Create/open a fixed string length datatset.
  ///
  /// \param Parent The group/node where the dataset is to be located.
  /// \param Name The name of the dataset.
  /// \param CMode Should the dataset be opened or created.
  /// \param StringSize What is the maximum number of characters in the string.
  /// \param ChunkSize The number of strings in one chunk.
  FixedSizeString(const hdf5::node::Group &Parent, std::string Name, Mode CMode,
                  size_t StringSize = 300, size_t ChunkSize = 1024);
  constexpr size_t getMaxStringSize() const { return MaxStringSize; };
  /// Append a new string to the dataset array
  void appendStringElement(std::string const &InString);

private:
  hdf5::datatype::String StringType;
  size_t MaxStringSize;
  size_t NrOfStrings{0};
};

class MultiDimDatasetBase : public hdf5::node::ChunkedDataset {
public:
  MultiDimDatasetBase() = default;

  /// \brief Open a dataset.
  ///
  /// Can only be used to open a dataset.
  /// \param Parent The group/node of the dataset in.
  /// \note This parameter is ignored when opening an existing dataset.
  /// \param CMode Should the dataset be opened or created.
  MultiDimDatasetBase(const hdf5::node::Group &Parent, Mode CMode)
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

  /// Append data to dataset that is contained in some sort of container.
  template <typename T>
  void appendArray(T const &NewData, hdf5::Dimensions Shape) {
    auto CurrentExtent = get_extent();
    hdf5::Dimensions Origin(CurrentExtent.size(), 0);
    Origin[0] = CurrentExtent[0];
    ++CurrentExtent[0];
    Shape.insert(Shape.begin(), 1);
    if (Shape.size() != CurrentExtent.size()) {
      LOG_ERROR(
          "Data has {} dimension(s) and dataset has {} (+1) dimensions.",
          Shape.size() - 1, CurrentExtent.size() - 1);
      throw std::runtime_error(
          "Rank (dimensions) of data to be written is wrong.");
    }
    for (size_t i = 1; i < Shape.size(); i++) {
      if (Shape[i] > CurrentExtent[i]) {
        LOG_WARN("Dimension {} of new data is larger than that of the "
                     "dataset. Extending dataset.",
                     i - 1);
        CurrentExtent[i] = Shape[i];
      } else if (Shape[i] < CurrentExtent[i]) {
        LOG_WARN("Dimension {} of new data is smaller than that of "
                     "the dataset. Using 0 as a filler.",
                     i - 1);
      }
    }
    Dataset::extent(CurrentExtent);
    hdf5::dataspace::Hyperslab Selection{{Origin}, {Shape}};
    write(NewData, Selection);
  }
};

/// h5cpp dataset class that implements methods for appending data.
template <class DataType> class MultiDimDataset : public MultiDimDatasetBase {
public:
  MultiDimDataset() = default;
  /// \brief Will create or open dataset with the given name.
  ///
  /// When opening a dataset, some of the paramaters will be ignored.
  ///
  /// \param Parent The group/node of the dataset in.
  /// \note This parameter is ignored when opening an existing dataset.
  /// \param CMode Should the dataset be opened or created.
  /// \param Shape The shape of the array in the NDArray. This vector
  /// will be prepended with one dimension to allow for adding of data.
  /// \param ChunkSize The chunk size (as number of elements) of the dataset,
  /// ignored if the dataset is opened.
  MultiDimDataset(hdf5::node::Group const &Parent, Mode CMode,
                  hdf5::Dimensions Shape, hdf5::Dimensions ChunkSize)
      : MultiDimDatasetBase() {
    if (Mode::Create == CMode) {
      Shape.insert(Shape.begin(), 0);
      hdf5::Dimensions MaxSize(Shape.size(),
                               hdf5::dataspace::Simple::UNLIMITED);
      std::vector<hsize_t> VectorChunkSize;
      if (ChunkSize.empty()) {
        LOG_WARN("No chunk size given. Using the default value 1024.");
        ChunkSize.emplace_back(1024);
      }
      if (ChunkSize.size() == Shape.size()) {
        VectorChunkSize = ChunkSize;
      } else if (ChunkSize.size() == 1 and Shape.size() > 1) {
        VectorChunkSize = Shape;
        auto ElementsPerRow =
            std::accumulate(std::next(Shape.begin()), Shape.end(), 1,
                            [](auto a, auto b) { return a * b; });
        auto NrOfRows = ChunkSize[0] / ElementsPerRow;
        if (NrOfRows == 0) {
          NrOfRows = 1;
        }
        VectorChunkSize[0] = NrOfRows;
      } else {
        LOG_ERROR("Unable to reconcile a data shape with {} dimensions "
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
  ///
  /// \param Parent The group/node of the dataset in.
  /// \note This parameter is ignored when opening an existing
  /// dataset.
  /// \param CMode Should the dataset be opened or created.
  MultiDimDataset(hdf5::node::Group const &Parent, Mode CMode)
      : MultiDimDatasetBase(Parent, CMode) {}
};

} // namespace NeXusDataset