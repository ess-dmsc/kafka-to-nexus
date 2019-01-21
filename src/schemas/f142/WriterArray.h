#pragma once

#include "../../logger.h"
#include "WriterTypedBase.h"
#include <numeric>

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// \brief  Implementation for array numeric types
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV> class WriterArray : public WriterTypedBase {
public:
  WriterArray(hdf5::node::Group HdfGroup, std::string const &SourceName,
              hsize_t ColumnCount, Value FlatbuffersValueTypeId,
              CollectiveQueue *cq);
  WriterArray(hdf5::node::Group HdfGroup, std::string const &SourceName,
              hsize_t ColumnCount, Value FlatbuffersValueTypeId,
              CollectiveQueue *cq, HDFIDStore *hdf_store);
  h5::append_ret write(FBUF const *fbuf) override;
  void storeLatestInto(std::string const &StoreLatestInto) override;
  uptr<h5::h5d_chunked_2d<DT>> ChunkedDataset;
  Value FlatbuffersValueTypeId = Value::NONE;
  size_t ChunkSize = 64 * 1024;
};

/// \brief  Create a new dataset for array numeric types
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV>
WriterArray<DT, FV>::WriterArray(hdf5::node::Group HdfGroup,
                                 std::string const &SourceName,
                                 hsize_t ColumnCount,
                                 Value FlatbuffersValueTypeId,
                                 CollectiveQueue *cq)
    : FlatbuffersValueTypeId(FlatbuffersValueTypeId) {
  if (ColumnCount <= 0) {
    throw std::runtime_error(fmt::format(
        "Can not handle number of columns ColumnCount == {}", ColumnCount));
  }
  LOG(spdlog::level::debug, "f142 init_impl  ColumnCount: {}", ColumnCount);
  ChunkedDataset = h5::h5d_chunked_2d<DT>::create(HdfGroup, SourceName,
                                                  ColumnCount, 64 * 1024, cq);
  if (ChunkedDataset == nullptr) {
    throw std::runtime_error(fmt::format(
        "Could not create hdf dataset  SourceName: {}", SourceName));
  }
}

/// \brief  Open a dataset for array numeric types
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV>
WriterArray<DT, FV>::WriterArray(hdf5::node::Group HdfGroup,
                                 std::string const &SourceName,
                                 hsize_t ColumnCount,
                                 Value FlatbuffersValueTypeId,
                                 CollectiveQueue *cq, HDFIDStore *hdf_store)
    : FlatbuffersValueTypeId(FlatbuffersValueTypeId) {
  if (ColumnCount <= 0) {
    throw std::runtime_error(fmt::format(
        "Can not handle number of columns ColumnCount == {}", ColumnCount));
  }
  LOG(spdlog::level::debug, "f142 writer_typed_array reopen  ColumnCount: {}",
      ColumnCount);
  ChunkedDataset = h5::h5d_chunked_2d<DT>::open(HdfGroup, SourceName,
                                                ColumnCount, cq, hdf_store);
  if (ChunkedDataset == nullptr) {
    throw std::runtime_error(
        fmt::format("Could not open hdf dataset  SourceName: {}", SourceName));
  }
  ChunkedDataset->buffer_init(ChunkSize, 0);
}

/// \brief  Write to a numeric array dataset
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV>
h5::append_ret WriterArray<DT, FV>::write(LogData const *fbuf) {
  h5::append_ret Result{h5::AppendResult::ERROR, 0, 0};
  auto vt = fbuf->value_type();
  if (vt == Value::NONE || vt != FlatbuffersValueTypeId) {
    Result.ErrorString =
        fmt::format("vt == Value::NONE || vt != FlatbuffersValueTypeId");
    return Result;
  }
  auto v1 = (FV const *)fbuf->value();
  if (!v1) {
    Result.ErrorString = fmt::format("value() in flatbuffer is nullptr");
    return Result;
  }
  auto v2 = v1->value();
  if (!v2) {
    Result.ErrorString =
        fmt::format("value() in value of flatbuffer is nullptr");
    return Result;
  }
  if (ChunkedDataset == nullptr) {
    Result.ErrorString = fmt::format("Dataset is nullptr");
    return Result;
  }
  return ChunkedDataset->append_data_2d(v2->data(), v2->size());
}

template <typename DT, typename FV>
void WriterArray<DT, FV>::storeLatestInto(std::string const &StoreLatestInto) {
  auto &Dataset = ChunkedDataset->ds.Dataset;
  auto Type = Dataset.datatype();
  auto SpaceSrc = hdf5::dataspace::Simple(Dataset.dataspace());
  auto DimSrc = SpaceSrc.current_dimensions();
  if (DimSrc.size() < 2) {
    throw std::runtime_error("unexpected dimensions");
  }
  auto DimMem = DimSrc;
  for (size_t I = 1; I < DimSrc.size(); ++I) {
    DimMem.at(I - 1) = DimMem.at(I);
  }
  DimMem.resize(DimMem.size() - 1);
  auto N = std::accumulate(DimMem.begin(), DimMem.end(), 1,
                           std::multiplies<size_t>());
  hdf5::Dimensions Offset;
  Offset.assign(DimSrc.size(), 0);
  if (DimSrc.at(0) == 0) {
    return;
  }
  hdf5::dataspace::Simple SpaceMem(DimMem);
  hdf5::node::Dataset Latest;
  try {
    Latest = Dataset.link().parent().get_dataset(StoreLatestInto);
  } catch (...) {
    Latest =
        Dataset.link().parent().create_dataset(StoreLatestInto, Type, SpaceMem);
  }
  Offset.at(0) = ChunkedDataset->size() - 1;
  hdf5::Dimensions Block = DimSrc;
  Block.at(0) = 1;
  SpaceSrc.selection(hdf5::dataspace::SelectionOperation::SET,
                     hdf5::dataspace::Hyperslab(Offset, Block));
  std::vector<char> Buffer(N * Type.size());
  Dataset.read(Buffer, Type, SpaceMem, SpaceSrc);
  Latest.write(Buffer, Type, SpaceMem, SpaceMem);
}
}
}
}
