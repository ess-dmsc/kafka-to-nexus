#include "WriterScalarString.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// \brief  Open or create a new dataset for scalar strings.
WriterScalarString::WriterScalarString(hdf5::node::Group const &HdfGroup,
                                       std::string const &SourceName,
                                       Mode OpenMode) {
  LOG(Sev::Debug, "f142 init_impl  WriterScalarString");
  if (OpenMode == Mode::Open) {
    ChunkedDataset = h5::Chunked1DString::open(HdfGroup, SourceName);
    if (ChunkedDataset == nullptr) {
      throw std::runtime_error(fmt::format(
          "Could not open hdf dataset  SourceName: {}", SourceName));
    }
  } else if (OpenMode == Mode::Create) {
    ChunkedDataset =
        h5::Chunked1DString::create(HdfGroup, SourceName, 64 * 1024);
    if (ChunkedDataset == nullptr) {
      throw std::runtime_error(fmt::format(
          "Could not create hdf dataset  SourceName: {}", SourceName));
    }
  }
}

/// \brief  Write to a scalar string dataset.
h5::append_ret WriterScalarString::write(LogData const *fbuf) {
  auto vt = fbuf->value_type();
  if (vt != Value::String) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  if (not flatbuffers::IsFieldPresent(fbuf, LogData::VT_VALUE)) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  if (ChunkedDataset == nullptr) {
    return {h5::AppendResult::ERROR, 0, 0};
  }
  return ChunkedDataset->append(
      static_cast<String const *>(fbuf->value())->value()->str());
}
} // namespace f142
} // namespace Schemas
} // namespace FileWriter
