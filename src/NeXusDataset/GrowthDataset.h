// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2024 European Spallation Source ERIC */

#pragma once
#include <NeXusDataset/ExtensibleDataset.h>

namespace NeXusDataset {
// A Multi dimensional dataset that can grow along the first (message) dimension
template <class DataType>
class GrowthDataset : public MultiDimDatasetBase {
  void _construct(hdf5::node::Group const &Parent, Mode CMode,
                  hdf5::Dimensions Shape, hdf5::Dimensions ChunkSize) {
    if (Mode::Create == CMode) {
      auto MaxSize = Shape;
      Shape.insert(Shape.begin(), 0);
      MaxSize.insert(MaxSize.begin(), hdf5::dataspace::Simple::unlimited);
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
          Parent, Name, hdf5::datatype::create<DataType>(),
          hdf5::dataspace::Simple(Shape, MaxSize), VectorChunkSize));
    } else if (Mode::Open == CMode) {
      Dataset::operator=(Parent.get_dataset(Name));
    } else {
      throw std::runtime_error(
          "MultiDimDataset::MultiDimDataset(): Unknown mode.");
    }
  }
public:
  GrowthDataset() = default;
  /// \brief Will create or open dataset with the given name.
  ///
  /// When opening a dataset, some of the paramaters will be ignored.
  ///
  /// \param Parent The group/node of the dataset in.
  /// \param CMode Should the dataset be opened or created.
  /// \note This parameter is ignored when opening an existing dataset.
  /// \param Shape The shape of the array in the NDArray. This vector
  /// will be prepended with one dimension to allow for adding of data.
  /// \param ChunkSize The chunk size (as number of elements) of the dataset,
  /// ignored if the dataset is opened.
  GrowthDataset(hdf5::node::Group const &Parent, std::string Name, Mode CMode,
                  hdf5::Dimensions Shape, hdf5::Dimensions ChunkSize)
      : MultiDimDatasetBase(std::move(Name)) {
    _construct(Parent, CMode, Shape, ChunkSize);
  }

  /// \brief Open a dataset.
  ///
  /// Can only be used to open a dataset.
  ///
  /// \param Parent The group/node of the dataset in.
  /// \note This parameter is ignored when opening an existing
  /// dataset.
  /// \param Name The name of the dataset.
  /// \param CMode Should the dataset be opened or created.
  GrowthDataset(hdf5::node::Group const &Parent, std::string Name, Mode CMode)
      : MultiDimDatasetBase(Parent, std::move(Name), CMode) {}
};

} // namespace NeXusDataset
