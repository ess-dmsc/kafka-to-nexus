// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <h5cpp/hdf5.hpp>

namespace MetaDataInternal {

class HDF5Storage {
public:
  HDF5Storage() = default;
  virtual ~HDF5Storage() = default;
  virtual void writeToFile(hdf5::node::Group){};
};

} // namespace MetaDataInternal

namespace MetaData {
using StoragePtr = std::unique_ptr<MetaDataInternal::HDF5Storage>;

}
