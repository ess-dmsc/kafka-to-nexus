#pragma once

#include "../../h5.h"
#include "Common.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// Interface for the writers of the different types.
class WriterTypedBase {
public:
  virtual ~WriterTypedBase() = default;
  virtual h5::append_ret write(FBUF const *fbuf) = 0;
  virtual void storeLatestInto(std::string const &StoreLatestInto) {}
};
}
}
}
