#pragma once

#include <h5cpp/hdf5.hpp>

namespace MetaDataInternal {

class HDF5Storage {
public:
  HDF5Storage() = default;
  virtual ~HDF5Storage() = default;
  virtual void writeToFile(hdf5::node::Group) {};
};

}

namespace MetaData {
using StoragePtr = std::unique_ptr<MetaDataInternal::HDF5Storage>;

}
