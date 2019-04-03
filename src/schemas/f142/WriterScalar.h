#pragma once

#include "../../logger.h"
#include "WriterTypedBase.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// \brief  Implementation for scalar numeric types.
///
/// \tparam  DT  The C datatype for this dataset.
/// \tparam  FV  The Flatbuffers datatype for this dataset.
template <typename DT, typename FV>
class WriterScalar : public WriterTypedBase {
public:
  WriterScalar(hdf5::node::Group HdfGroup, std::string const &SourceName,
               Value FlatbuffersValueTypeId, Mode OpenMode);
  h5::append_ret write(FBUF const *fbuf) override;
  void storeLatestInto(std::string const &StoreLatestInto) override;
  uptr<h5::h5d_chunked_1d<DT>> ChunkedDataset;
  Value FlatbuffersValueTypeId = Value::NONE;
  size_t ChunkSize = 64 * 1024;
  SharedLogger Logger = spdlog::get("filewriterlogger");
};

/// \brief  Open or create a new dataset for scalar numeric types.
///
/// \tparam  DT  The C datatype for this dataset.
/// \tparam  FV  The Flatbuffers datatype for this dataset.
template <typename DT, typename FV>
WriterScalar<DT, FV>::WriterScalar(hdf5::node::Group HdfGroup,
                                   std::string const &SourceName,
                                   Value FlatbuffersValueTypeId, Mode OpenMode)
    : FlatbuffersValueTypeId(FlatbuffersValueTypeId) {
  Logger->trace("f142 WriterScalar ctor");
  if (OpenMode == Mode::Open) {
    ChunkedDataset = h5::h5d_chunked_1d<DT>::open(HdfGroup, SourceName);
    if (ChunkedDataset == nullptr) {
      throw std::runtime_error(fmt::format(
          "Could not open hdf dataset  SourceName: {}", SourceName));
    }
    ChunkedDataset->buffer_init(ChunkSize, 0);
  } else if (OpenMode == Mode::Create) {
    ChunkedDataset =
        h5::h5d_chunked_1d<DT>::create(HdfGroup, SourceName, ChunkSize);
    if (ChunkedDataset == nullptr) {
      throw std::runtime_error(fmt::format(
          "Could not create hdf dataset  SourceName: {}", SourceName));
    }
  }
}

/// \brief  Write to a numeric scalar dataset
///
/// \tparam  DT  The C datatype for this dataset.
/// \tparam  FV  The Flatbuffers datatype for this dataset.
template <typename DT, typename FV>
h5::append_ret WriterScalar<DT, FV>::write(LogData const *Buffer) {
  h5::append_ret Result{h5::AppendResult::ERROR, 0, 0};
  auto ValueType = Buffer->value_type();
  if (ValueType == Value::NONE || ValueType != FlatbuffersValueTypeId) {
    Result.ErrorString = fmt::format(
        "ValueType == Value::NONE || ValueType != FlatbuffersValueTypeId");
    return Result;
  }
  auto ValueMember = reinterpret_cast<FV const *>(Buffer->value());
  if (!ValueMember) {
    Result.ErrorString = fmt::format("value() in flatbuffer is nullptr");
    return Result;
  }
  auto Value = ValueMember->value();
  if (ChunkedDataset == nullptr) {
    Result.ErrorString = fmt::format("Dataset is nullptr");
    return Result;
  }
  return ChunkedDataset->append_data_1d(&Value, 1);
}

template <typename DT, typename FV>
void WriterScalar<DT, FV>::storeLatestInto(std::string const &StoreLatestInto) {
  auto &Dataset = ChunkedDataset->DataSet.Dataset;
  auto Type = Dataset.datatype();
  auto SpaceSrc = hdf5::dataspace::Simple(Dataset.dataspace());
  auto DimSrc = SpaceSrc.current_dimensions();
  if (DimSrc.size() < 1) {
    throw std::runtime_error("unexpected dimensions");
  }
  auto DimMem = DimSrc;
  for (size_t I = 1; I < DimSrc.size(); ++I) {
    DimMem.at(I - 1) = DimMem.at(I);
  }
  DimMem.resize(DimMem.size() - 1);
  size_t N = 1;
  for (size_t I = 0; I < DimMem.size(); ++I) {
    N *= DimMem.at(I);
  }
  hdf5::Dimensions Offset;
  Offset.resize(DimSrc.size());
  for (size_t I = 0; I < Offset.size(); ++I) {
    Offset.at(I) = 0;
  }
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
} // namespace f142
} // namespace Schemas
} // namespace FileWriter
