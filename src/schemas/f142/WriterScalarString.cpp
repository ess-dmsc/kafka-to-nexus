#include "WriterScalarString.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// \brief  Create a new dataset for scalar strings.
WriterScalarString::WriterScalarString(hdf5::node::Group hdf_group,
                                       std::string const &source_name,
                                       Value fb_value_type_id,
                                       CollectiveQueue *cq) {
  LOG(Sev::Debug, "f142 init_impl  WriterScalarString");
  this->ds = h5::Chunked1DString::create(hdf_group, source_name, 64 * 1024, cq);
  if (!this->ds) {
    LOG(Sev::Error, "could not create hdf dataset  source_name: {}",
        source_name);
  }
}

/// \brief  Open a dataset for scalar strings.
WriterScalarString::WriterScalarString(hdf5::node::Group hdf_group,
                                       std::string const &source_name,
                                       Value fb_value_type_id,
                                       CollectiveQueue *cq,
                                       HDFIDStore *hdf_store) {
  LOG(Sev::Debug, "f142 init_impl  WriterScalarString");
  ds = h5::Chunked1DString::open(hdf_group, source_name, cq, hdf_store);
  if (!this->ds) {
    LOG(Sev::Error, "could not create hdf dataset  source_name: {}",
        source_name);
  }
}

/// \brief  Write to a scalar string dataset.
h5::append_ret WriterScalarString::write_impl(LogData const *fbuf) {
  auto vt = fbuf->value_type();
  if (vt != Value::String) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v1 = static_cast<String const *>(fbuf->value());
  if (!v1) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v2 = v1->value();
  if (!this->ds) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  return this->ds->append(v2->str());
}
}
}
}
