#pragma once

#include "../../logger.h"
#include "WriterTypedBase.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

template <typename DT, typename FV>
class WriterScalar : public WriterTypedBase {
public:
  WriterScalar(hdf5::node::Group hdf_group, std::string const &source_name,
               Value fb_value_type_id, CollectiveQueue *cq);
  WriterScalar(hdf5::node::Group hdf_group, std::string const &source_name,
               Value fb_value_type_id, CollectiveQueue *cq,
               HDFIDStore *hdf_store);
  h5::append_ret write_impl(FBUF const *fbuf) override;
  uptr<h5::h5d_chunked_1d<DT>> ds;
  Value _fb_value_type_id = Value::NONE;
};

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
}
}
}
