#pragma once

#include "../../logger.h"
#include "WriterTypedBase.h"

template <typename T> using uptr = std::unique_ptr<T>;

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
h5::append_ret WriterScalar<DT, FV>::write_impl(LogData const *fbuf) {
  auto vt = fbuf->value_type();
  if (vt == Value::NONE || vt != _fb_value_type_id) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v1 = (FV const *)fbuf->value();
  if (!v1) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v2 = v1->value();
  if (!this->ds) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  return this->ds->append_data_1d(&v2, 1);
}
}
}
}
