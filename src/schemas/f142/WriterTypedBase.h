#pragma once

#include "../../h5.h"
#include "Common.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

class WriterTypedBase {
public:
  virtual ~WriterTypedBase() {}
  virtual h5::append_ret write_impl(FBUF const *fbuf) = 0;
};
}
}
}
