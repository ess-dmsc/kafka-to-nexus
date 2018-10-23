#pragma once

#include "../../logger.h"
#include "WriterTypedBase.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// \brief  Implementation for scalar strings
class WriterScalarString : public WriterTypedBase {
public:
  WriterScalarString(hdf5::node::Group hdf_group,
                     std::string const &source_name,
                     Value FlatbuffersValueTypeId, CollectiveQueue *cq);
  WriterScalarString(hdf5::node::Group hdf_group,
                     std::string const &source_name,
                     Value FlatbuffersValueTypeId, CollectiveQueue *cq,
                     HDFIDStore *hdf_store);
  h5::append_ret write(FBUF const *fbuf) override;
  h5::Chunked1DString::ptr ChunkedDataset;
  Value FlatbuffersValueTypeId = Value::String;
};
}
}
}
