#pragma once

#include "../../logger.h"
#include "WriterTypedBase.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// Implementation for scalar strings.
class WriterScalarString : public WriterTypedBase {
public:
  WriterScalarString(hdf5::node::Group const &HdfGroup,
                     std::string const &SourceName,
                     Value FlatbuffersValueTypeId, Mode OpenMode);
  h5::append_ret write(FBUF const *fbuf) override;
  h5::Chunked1DString::ptr ChunkedDataset;
  Value FlatbuffersValueTypeId = Value::String;
};
} // namespace f142
} // namespace Schemas
} // namespace FileWriter
