#pragma once

#include "../../logger.h"
#include "WriterTypedBase.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

template <typename DT, typename FV> class WriterArray : public WriterTypedBase {
public:
  WriterArray(hdf5::node::Group hdf_group, std::string const &source_name,
              hsize_t ncols, Value fb_value_type_id, CollectiveQueue *cq);
  WriterArray(hdf5::node::Group, std::string const &source_name, hsize_t ncols,
              Value fb_value_type_id, CollectiveQueue *cq,
              HDFIDStore *hdf_store);
  h5::append_ret write_impl(FBUF const *fbuf) override;
  uptr<h5::h5d_chunked_2d<DT>> ds;
  Value _fb_value_type_id = Value::NONE;
};

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

template <typename DT, typename FV>
h5::append_ret WriterArray<DT, FV>::write_impl(LogData const *fbuf) {
  auto vt = fbuf->value_type();
  if (vt == Value::NONE || vt != _fb_value_type_id) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v1 = (FV const *)fbuf->value();
  if (!v1) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v2 = v1->value();
  if (!v2) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  if (!this->ds) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  return this->ds->append_data_2d(v2->data(), v2->size());
}
}
}
}
