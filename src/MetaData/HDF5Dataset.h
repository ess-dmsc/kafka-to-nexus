// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "HDF5Storage.h"
#include <string>

namespace MetaData {

class HDF5Dataset : public MetaDataInternal::HDF5Storage {
public:
  explicit HDF5Dataset(std::string Path);
};

} // namespace MetaData