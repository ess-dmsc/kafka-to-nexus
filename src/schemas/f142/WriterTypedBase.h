#pragma once

#include "../../h5.h"
#include "Common.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// \brief  Interface for the writers of the different types
class WriterTypedBase {
public:
  virtual ~WriterTypedBase() = default;
  virtual h5::append_ret write_impl(FBUF const *fbuf) = 0;
};
}
}
}
