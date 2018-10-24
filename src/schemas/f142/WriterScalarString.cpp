#include "WriterScalarString.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// \brief  Create a new dataset for scalar stringss
WriterScalarString::WriterScalarString(hdf5::node::Group HdfGroup,
                                       std::string const &SourceName,
                                       Value fb_value_type_id,
                                       CollectiveQueue *cq) {
  LOG(Sev::Debug, "f142 init_impl  WriterScalarString");
  ChunkedDataset =
      h5::Chunked1DString::create(HdfGroup, SourceName, 64 * 1024, cq);
  if (ChunkedDataset == nullptr) {
    LOG(Sev::Error, "could not create hdf dataset  SourceName: {}", SourceName);
  }
}

/// \brief  Open a dataset for scalar strings
WriterScalarString::WriterScalarString(hdf5::node::Group HdfGroup,
                                       std::string const &SourceName,
                                       Value fb_value_type_id,
                                       CollectiveQueue *cq,
                                       HDFIDStore *hdf_store) {
  LOG(Sev::Debug, "f142 init_impl  WriterScalarString");
  ChunkedDataset =
      h5::Chunked1DString::open(HdfGroup, SourceName, cq, hdf_store);
  if (ChunkedDataset == nullptr) {
    LOG(Sev::Error, "could not create hdf dataset  SourceName: {}", SourceName);
  }
}

/// \brief  Write to a scalar string dataset
h5::append_ret WriterScalarString::write(LogData const *fbuf) {
  auto vt = fbuf->value_type();
  if (vt != Value::String) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v1 = static_cast<String const *>(fbuf->value());
  if (!v1) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  auto v2 = v1->value();
  if (ChunkedDataset == nullptr) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  return ChunkedDataset->append(v2->str());
}
}
}
}
