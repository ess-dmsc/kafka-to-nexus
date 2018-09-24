#pragma once

#include "../../logger.h"
#include "WriterTypedBase.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// \brief  Implementation for scalar numeric types
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV>
class WriterScalar : public WriterTypedBase {
public:
  WriterScalar(hdf5::node::Group hdf_group, std::string const &source_name,
               Value fb_value_type_id, CollectiveQueue *cq);
  WriterScalar(hdf5::node::Group hdf_group, std::string const &source_name,
               Value fb_value_type_id, CollectiveQueue *cq,
               HDFIDStore *hdf_store);
  h5::append_ret write_impl(FBUF const *fbuf) override;
  void storeLatestInto(std::string const &StoreLatestInto) override;
  uptr<h5::h5d_chunked_1d<DT>> ds;
  Value _fb_value_type_id = Value::NONE;
};

/// \brief  Create a new dataset for scalar numeric types
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV>
WriterScalar<DT, FV>::WriterScalar(hdf5::node::Group hdf_group,
                                   std::string const &source_name,
                                   Value fb_value_type_id, CollectiveQueue *cq)
    : _fb_value_type_id(fb_value_type_id) {
  LOG(Sev::Debug, "f142 WriterScalar ctor");
  this->ds =
      h5::h5d_chunked_1d<DT>::create(hdf_group, source_name, 64 * 1024, cq);
  if (!this->ds) {
    LOG(Sev::Error, "could not create hdf dataset  source_name: {}",
        source_name);
  }
}

/// \brief  Open a dataset for scalar numeric types
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV>
WriterScalar<DT, FV>::WriterScalar(hdf5::node::Group hdf_group,
                                   std::string const &source_name,
                                   Value fb_value_type_id, CollectiveQueue *cq,
                                   HDFIDStore *hdf_store)
    : _fb_value_type_id(fb_value_type_id) {
  LOG(Sev::Debug, "f142 WriterScalar ctor");
  ds = h5::h5d_chunked_1d<DT>::open(hdf_group, source_name, cq, hdf_store);
  if (!this->ds) {
    LOG(Sev::Error, "could not create hdf dataset  source_name: {}",
        source_name);
  }
  // TODO take from config
  ds->buffer_init(64 * 1024, 0);
}

/// \brief  Write to a numeric scalar dataset
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV>
h5::append_ret WriterScalar<DT, FV>::write_impl(LogData const *Buffer) {
  h5::append_ret Result{h5::AppendResult::ERROR, 0, 0};
  auto ValueType = Buffer->value_type();
  if (ValueType == Value::NONE || ValueType != _fb_value_type_id) {
    Result.ErrorString = fmt::format(
        "ValueType == Value::NONE || ValueType != _fb_value_type_id");
    return Result;
  }
  auto ValueMember = (FV const *)Buffer->value();
  if (!ValueMember) {
    Result.ErrorString = fmt::format("value() in flatbuffer is nullptr");
    return Result;
  }
  auto Value = ValueMember->value();
  if (!this->ds) {
    Result.ErrorString = fmt::format("Dataset is nullptr");
    return Result;
  }
  return this->ds->append_data_1d(&Value, 1);
}

template <typename DT, typename FV>
void WriterScalar<DT, FV>::storeLatestInto(std::string const &StoreLatestInto) {
  LOG(Sev::Critical, "LATEST WRITER");
  auto &Dataset = ds->ds.Dataset;
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
  Offset.at(0) = ds->ds.snow.at(0) - 1;
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
