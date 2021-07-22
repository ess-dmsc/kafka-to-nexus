#pragma once

#include "HDF5Storage.h"
#include <string>

namespace MetaData {

class HDF5Dataset : public MetaDataInternal::HDF5Storage {
public:
  explicit HDF5Dataset(std::string Path);
};

} // namespace MetaData