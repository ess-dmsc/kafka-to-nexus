#pragma once

#include "../../logger.h"
#include "WriterTypedBase.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// \brief  Implementation for array numeric types
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV> class WriterArray : public WriterTypedBase {
public:
  WriterArray(hdf5::node::Group hdf_group, std::string const &source_name,
              hsize_t ncols, Value fb_value_type_id, CollectiveQueue *cq);
  WriterArray(hdf5::node::Group, std::string const &source_name, hsize_t ncols,
              Value fb_value_type_id, CollectiveQueue *cq,
              HDFIDStore *hdf_store);
  h5::append_ret write_impl(FBUF const *fbuf) override;
  void storeLatestInto(std::string const &StoreLatestInto) override;
  uptr<h5::h5d_chunked_2d<DT>> ds;
  Value _fb_value_type_id = Value::NONE;
};

/// \brief  Create a new dataset for array numeric types
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV>
WriterArray<DT, FV>::WriterArray(hdf5::node::Group hdf_group,
                                 std::string const &source_name, hsize_t ncols,
                                 Value fb_value_type_id, CollectiveQueue *cq)
    : _fb_value_type_id(fb_value_type_id) {
  if (ncols <= 0) {
    LOG(Sev::Error, "can not handle number of columns ncols == {}", ncols);
    return;
  }
  LOG(Sev::Debug, "f142 init_impl  ncols: {}", ncols);
  this->ds = h5::h5d_chunked_2d<DT>::create(hdf_group, source_name, ncols,
                                            64 * 1024, cq);
  if (!this->ds) {
    LOG(Sev::Error,
        "could not create hdf dataset  source_name: {}  number of columns: {}",
        source_name, ncols);
  }
}

/// \brief  Open a dataset for array numeric types
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV>
WriterArray<DT, FV>::WriterArray(hdf5::node::Group hdf_group,
                                 std::string const &source_name, hsize_t ncols,
                                 Value fb_value_type_id, CollectiveQueue *cq,
                                 HDFIDStore *hdf_store)
    : _fb_value_type_id(fb_value_type_id) {
  if (ncols <= 0) {
    LOG(Sev::Error, "can not handle number of columns ncols == {}", ncols);
    return;
  }
  LOG(Sev::Debug, "f142 writer_typed_array reopen  ncols: {}", ncols);
  ds = h5::h5d_chunked_2d<DT>::open(hdf_group, source_name, ncols, cq,
                                    hdf_store);
  if (!ds) {
    LOG(Sev::Error,
        "could not create hdf dataset  source_name: {}  number of columns: {}",
        source_name, ncols);
    return;
  }
  // TODO take from config
  ds->buffer_init(64 * 1024, 0);
}

/// \brief  Write to a numeric array dataset
///
/// \tparam  DT  The C datatype for this dataset
/// \tparam  FV  The Flatbuffers datatype for this dataset
template <typename DT, typename FV>
h5::append_ret WriterArray<DT, FV>::write_impl(LogData const *fbuf) {
  h5::append_ret Result{h5::AppendResult::ERROR, 0, 0};
  auto vt = fbuf->value_type();
  if (vt == Value::NONE || vt != _fb_value_type_id) {
    Result.ErrorString =
        fmt::format("vt == Value::NONE || vt != _fb_value_type_id");
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
  if (!this->ds) {
    Result.ErrorString = fmt::format("Dataset is nullptr");
    return Result;
  }
  return this->ds->append_data_2d(v2->data(), v2->size());
}

template <typename DT, typename FV>
void WriterArray<DT, FV>::storeLatestInto(std::string const &StoreLatestInto) {
  auto &Dataset = ds->ds.Dataset;
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
