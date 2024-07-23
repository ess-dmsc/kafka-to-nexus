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

namespace hdf5::datatype {

/// \brief Required for h5cpp to save data of type std::int8_t const.
template <> class TypeTrait<std::int8_t const> {
public:
  using Type = std::int8_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_INT8))};
  }
};

/// \brief Required for h5cpp to save data of type std::uint8_t const
template <> class TypeTrait<std::uint8_t const> {
public:
  using Type = std::uint8_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_UINT8))};
  }
};

/// \brief Required for h5cpp to save data of type std::int16_t const
template <> class TypeTrait<std::int16_t const> {
public:
  using Type = std::int16_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_INT16))};
  }
};

/// \brief Required for h5cpp to save data of type std::uint16_t const
template <> class TypeTrait<std::uint16_t const> {
public:
  using Type = std::uint16_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_UINT16))};
  }
};

/// \brief Required for h5cpp to save data of type std::int32_t const
template <> class TypeTrait<std::int32_t const> {
public:
  using Type = std::int32_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_INT32))};
  }
};

/// \brief Required for h5cpp to save data of type std::uint32_t const
template <> class TypeTrait<std::uint32_t const> {
public:
  using Type = std::uint32_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_UINT32))};
  }
};

/// \brief Required for h5cpp to save data of type float const
template <> class TypeTrait<float const> {
public:
  using Type = float;
  using TypeClass = Float;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_FLOAT))};
  }
};

/// \brief Required for h5cpp to save data of type double const
template <> class TypeTrait<double const> {
public:
  using Type = double;
  using TypeClass = Float;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_DOUBLE))};
  }
};

/// \brief Required for h5cpp to save data of type char const
template <> class TypeTrait<char const> {
public:
  using Type = char;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_CHAR))};
  }
};

/// \brief Required for h5cpp to save data of type std::int64_t const
template <> class TypeTrait<std::int64_t const> {
public:
  using Type = std::int64_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_INT64))};
  }
};

/// \brief Required for h5cpp to save data of type std::uint64_t const
template <> class TypeTrait<std::uint64_t const> {
public:
  using Type = std::uint64_t;
  using TypeClass = Integer;
  static TypeClass create(const Type & = Type()) {
    return {ObjectHandle(H5Tcopy(H5T_NATIVE_UINT64))};
  }
};
} // namespace hdf5::datatype

namespace NeXusDataset {

enum class Mode { Create, Open };

/// \brief The base class for representing an extensible (i.e. it can grow) HDF5
/// dataset.
///
/// This base class is used in order to have templated child classes that can
/// override/inherit the member functions of this class.
class ExtensibleDatasetBase {
public:
  /// \brief Constructor.
  ExtensibleDatasetBase() = default;

  /// \brief Open a dataset.
  ///
  /// \param parent The group/node where the dataset to be opened is located.
  /// \param name The name of the dataset in the HDF5 structure.
  /// \param mode If the dataset should be created or opened. Note that it is
  /// not possible to create a dataset with this class. \throws
  /// std::runtime_error if creation of a dataset is attempted.
  ExtensibleDatasetBase(hdf5::node::Group const &parent,
                        std::string const &name, Mode mode) {
    if (Mode::Create == mode) {
      throw std::runtime_error(
          "ExtensibleDatasetBase::ExtensibleDatasetBase(): "
          "Can only open datasets, not create.");
    } else if (Mode::Open == mode) {
      _dataset = parent.get_dataset(name);
    } else {
      throw std::runtime_error(
          "ExtensibleDatasetBase::ExtensibleDatasetBase(): Unknown mode.");
    }
  }

  /// Gets the current size of the dataset.
  [[nodiscard]] hssize_t current_size() const {
    return _dataset.dataspace().size();
  }

  /// Read the current data in the dataset.
  /// Only for use in tests.
  ///
  /// \param buffer
  template <typename T> void read_data(std::vector<T> &buffer) {
    _dataset.read(buffer);
  }

  /// Read the current data in the dataset.
  /// Only for use in tests.
  ///
  /// \param buffer
  template <typename T> void read_data(T &buffer) { _dataset.read(buffer); }

  /// Access the underlying dataset.
  /// Only for use in tests.
  [[nodiscard]] hdf5::node::Dataset const &dataset() const { return _dataset; }

  /// Append data to dataset that is contained in some sort of container.
  template <typename T> void appendArray(T const &data) {
    _dataset.extent(0, data.size());
    hdf5::dataspace::Hyperslab selection{
        {NrOfElements}, {static_cast<unsigned long long>(data.size())}};
    _dataset.write(data, selection);
    NrOfElements += data.size();
  }

  /// Append single scalar values to dataset.
  template <typename T> void appendElement(T const &element) {
    _dataset.extent(0, 1);
    hdf5::dataspace::Hyperslab selection{{NrOfElements}, {1}};
    _dataset.write(element, selection);
    NrOfElements += 1;
  }

  template <class DataType>
  void appendArray(hdf5::ArrayAdapter<const DataType> const &data) {
    if (data.size() == 0) {
      return;
    }
    NewDimensions[0] = NrOfElements + data.size();
    _dataset.resize(NewDimensions);
    ArraySelection.offset({NrOfElements});
    ArraySelection.block({static_cast<unsigned long long>(data.size())});

    ArrayDataSpace.dimensions({data.size()}, {data.size()});
    hdf5::dataspace::Dataspace file_space = _dataset.dataspace();
    file_space.selection(hdf5::dataspace::SelectionOperation::Set,
                         ArraySelection);
    hdf5::datatype::Datatype array_value_type{
        hdf5::datatype::create(DataType())};
    _dataset.write(data, array_value_type, ArrayDataSpace, file_space, Dtpl);

    NrOfElements += data.size();
  }

  [[nodiscard]] size_t size() const { return NrOfElements; }

  /// \brief Read an attribute from the dataset.
  ///
  /// Note: only for use in tests!
  template <typename T>
  void attribute(std::string const &name, T &result) const {
    _dataset.attributes[name].read(result);
  }

  /// \brief Check if an attribute exists/
  ///
  /// Note: only for use in tests!
  [[nodiscard]] bool attribute_exists(std::string const &name) const {
    return _dataset.attributes.exists(name);
  }

  /// \brief Read data from the dataset.
  ///
  /// Note: only for use in tests!
  template <typename T> void read(T &result) { _dataset.read(result); }

protected:
  hdf5::node::Dataset _dataset;
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
      _dataset = hdf5::node::ChunkedDataset(
          Parent, Name, hdf5::datatype::create<DataType>(),
          hdf5::dataspace::Simple({0}, {hdf5::dataspace::Simple::unlimited}),
          {
              static_cast<unsigned long long>(ChunkSize),
          });
    } else if (Mode::Open == CMode) {
      _dataset = Parent.get_dataset(Name);
      NrOfElements = static_cast<size_t>(_dataset.dataspace().size());
    } else {
      throw std::runtime_error(
          "ExtensibleDataset::ExtensibleDataset(): Unknown mode.");
    }
  }
};

/// \brief
class FixedSizeString {
public:
  FixedSizeString() = default;
  /// \brief Create/open a fixed string length datatset.
  ///
  /// \param Parent The group/node where the dataset is to be located.
  /// \param Name The name of the dataset.
  /// \param CMode Should the dataset be opened or created.
  /// \param StringSize What is the maximum number of characters in the string.
  /// \param ChunkSize The number of strings in one chunk.
  FixedSizeString(hdf5::node::Group const &Parent, std::string const &Name,
                  Mode CMode, size_t StringSize = 300, size_t ChunkSize = 1024);

  /// \brief Get max string size.
  ///
  /// \return The max string size.
  [[nodiscard]] constexpr size_t getMaxStringSize() const {
    return MaxStringSize;
  };

  /// \brief Append a new string to the dataset array
  ///
  /// \param InString The string that is to be appended to the dataset.
  void appendStringElement(std::string const &InString);

  /// Gets the current size of the dataset.
  [[nodiscard]] hssize_t current_size() const {
    return _dataset.dataspace().size();
  }

  /// \brief Read a string element from the dataset array.
  ///
  /// Only exists for maintaining back-compatibility with unit tests.
  /// Delete when a better way to test is found!
  ///
  /// \param offset The index of the element to read.
  /// \return The string value.
  [[nodiscard]] std::string read_element(uint64_t offset) const {
    std::string result;
    _dataset.read(result, _dataset.datatype(), hdf5::dataspace::Scalar(),
                  hdf5::dataspace::Hyperslab{{offset}, {1}});
    return result;
  }

private:
  hdf5::node::Dataset _dataset;
  hdf5::datatype::String StringType;
  size_t MaxStringSize{300};
  size_t NrOfStrings{0};
};

class MultiDimDatasetBase {
public:
  MultiDimDatasetBase() = default;

  /// \brief Open a dataset.
  ///
  /// \param Parent The group/node where the dataset to be opened is located.
  /// \param Name Tha name of the dataset in the HDF5 structure.
  /// \param CMode If the dataset should be created or opened. Note that it is
  /// not possible to create a dataset with this class. \throws
  /// std::runtime_error If creation of a dataset is attempted.
  MultiDimDatasetBase(hdf5::node::Group const &parent, std::string const &name,
                      Mode CMode) {
    if (Mode::Create == CMode) {
      throw std::runtime_error("MultiDimDatasetBase::MultiDimDatasetBase(): "
                               "Can only open datasets, not create.");
    } else if (Mode::Open == CMode) {
      _dataset = parent.get_dataset(name);
    } else {
      throw std::runtime_error(
          "MultiDimDatasetBase::MultiDimDatasetBase(): Unknown mode.");
    }
  }

  [[nodiscard]] hdf5::datatype::Datatype datatype() const {
    return _dataset.datatype();
  }

  [[nodiscard]] std::vector<hsize_t> chunk_info() const {
    return _dataset.creation_list().chunk();
  }

  /// \brief Read an attribute from the dataset.
  ///
  /// Note: only for use in tests!
  template <typename T>
  void attribute(std::string const &name, T &result) const {
    _dataset.attributes[name].read(result);
  }

  [[nodiscard]] bool attribute_exists(std::string const &name) const {
    return _dataset.attributes.exists(name);
  }

  /// \brief Get the dimensions of the dataset.
  [[nodiscard]] std::vector<hsize_t> dimensions() const {
    return hdf5::dataspace::Simple(_dataset.dataspace()).current_dimensions();
  }

  /// \brief Get the maximum dimensions of the dataset.
  [[nodiscard]] std::vector<hsize_t> max_dimensions() const {
    return hdf5::dataspace::Simple(_dataset.dataspace()).maximum_dimensions();
  }

  /// \brief Read data from the dataset.
  ///
  /// Note: only for use in tests!
  template <typename T> void read(T &result) { _dataset.read(result); }

  /// \brief Append data to dataset that is contained in some sort of container.
  ///
  /// \tparam T The data type of the data to be added.
  /// \param NewData The new data.
  /// \param Shape The shape of the new data.
  /// \throws std::runtime_error A basic check of the rank (number of
  /// dimensions) of the new data is done and an error is thrown if this is not
  /// correct for the current dataset.
  template <typename T>
  void appendArray(T const &NewData, hdf5::Dimensions Shape) {
    auto CurrentExtent = dimensions();
    hdf5::Dimensions Origin(CurrentExtent.size(), 0);
    Origin[0] = CurrentExtent[0];
    ++CurrentExtent[0];
    Shape.insert(Shape.begin(), 1);
    if (Shape.size() != CurrentExtent.size()) {
      Logger::Error(
          "Data has {} dimension(s) and dataset has {} (+1) dimensions.",
          Shape.size() - 1, CurrentExtent.size() - 1);
      throw std::runtime_error(
          "Rank (dimensions) of data to be written is wrong.");
    }
    for (size_t i = 1; i < Shape.size(); i++) {
      if (Shape[i] > CurrentExtent[i]) {
        Logger::Info("Dimension {} of new data is larger than that of the "
                     "dataset. Extending dataset.",
                     i - 1);
        CurrentExtent[i] = Shape[i];
      } else if (Shape[i] < CurrentExtent[i]) {
        Logger::Info("Dimension {} of new data is smaller than that of "
                     "the dataset. Using 0 as a filler.",
                     i - 1);
      }
    }
    _dataset.extent(CurrentExtent);
    hdf5::dataspace::Hyperslab Selection{{Origin}, {Shape}};
    _dataset.write(NewData, Selection);
  }

protected:
  hdf5::node::Dataset _dataset;
};

/// \brief h5cpp dataset class that implements methods for appending data.
///
/// \tparam DataType The (primitive) type that is (to be) stored in the dataset.
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
  MultiDimDataset(hdf5::node::Group const &parent, std::string const &name,
                  Mode CMode, hdf5::Dimensions shape,
                  hdf5::Dimensions chunksize)
      : MultiDimDatasetBase() {
    if (Mode::Create == CMode) {
      shape.insert(shape.begin(), 0);
      hdf5::Dimensions MaxSize(shape.size(),
                               hdf5::dataspace::Simple::unlimited);
      std::vector<hsize_t> VectorChunkSize;
      if (chunksize.empty()) {
        Logger::Info("No chunk size given. Using the default value 1024.");
        chunksize.emplace_back(1024);
      }
      if (chunksize.size() == shape.size()) {
        VectorChunkSize = chunksize;
      } else if (chunksize.size() == 1 && shape.size() > 1) {
        VectorChunkSize = shape;
        auto ElementsPerRow =
            std::accumulate(std::next(shape.begin()), shape.end(), 1,
                            [](auto a, auto b) { return a * b; });
        auto NrOfRows = chunksize[0] / ElementsPerRow;
        if (NrOfRows == 0) {
          NrOfRows = 1;
        }
        VectorChunkSize[0] = NrOfRows;
      } else {
        Logger::Error("Unable to reconcile a data shape with {} dimensions "
                      "and chunk size with {} dimensions. Using default "
                      "values.",
                      shape.size(), chunksize.size());
        VectorChunkSize = shape;
        VectorChunkSize[0] = 1024;
      }
      _dataset = hdf5::node::ChunkedDataset(
          parent, name, hdf5::datatype::create<DataType>(),
          hdf5::dataspace::Simple(shape, MaxSize), VectorChunkSize);
    } else if (Mode::Open == CMode) {
      _dataset = parent.get_dataset(name);
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
  MultiDimDataset(hdf5::node::Group const &parent, std::string const &name,
                  Mode CMode)
      : MultiDimDatasetBase(parent, name, CMode) {}
};

} // namespace NeXusDataset