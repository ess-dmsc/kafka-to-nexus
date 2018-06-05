#pragma once

template <typename T> using uptr = std::unique_ptr<T>;

namespace FileWriter {
namespace Schemas {
namespace f142 {

#include "schemas/f142_logdata_generated.h"

using FBUF = LogData;

class WriterTypedBase {
public:
  virtual h5::append_ret write_impl(FBUF const *fbuf) = 0;
};
}
}
}
