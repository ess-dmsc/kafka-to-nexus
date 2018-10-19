#pragma once

#include "../../logger.h"
#include "WriterTypedBase.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// Implementation for scalar strings.
class WriterScalarString : public WriterTypedBase {
public:
  WriterScalarString(hdf5::node::Group hdf_group,
                     std::string const &source_name, Value fb_value_type_id,
                     CollectiveQueue *cq);
  WriterScalarString(hdf5::node::Group hdf_group,
                     std::string const &source_name, Value fb_value_type_id,
                     CollectiveQueue *cq, HDFIDStore *hdf_store);
  h5::append_ret write_impl(FBUF const *fbuf) override;
  h5::Chunked1DString::ptr ds;
  Value _fb_value_type_id = Value::String;
};
}
}
}
