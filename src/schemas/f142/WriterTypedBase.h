#pragma once

#include "../../h5.h"
#include "Common.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

enum class Mode { Create, Open };

/// Interface for the writers of the different types.
class WriterTypedBase {
public:
  virtual ~WriterTypedBase() = default;
  virtual h5::append_ret write(FBUF const *fbuf) = 0;
  virtual void storeLatestInto(std::string const &StoreLatestInto) = 0;
};
} // namespace f142
} // namespace Schemas
} // namespace FileWriter
