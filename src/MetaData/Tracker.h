// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include "Value.h"
#include <h5cpp/hdf5.hpp>
#include <memory>
#include <mutex>

namespace MetaData {

using TrackerPtr = std::shared_ptr<Tracker>;

/// \brief Used to store the pointers to metadata variables.
///
/// The (almost) sole for this class to exist is so that we can automatically
/// write (or do whatever) to the metadata when its destructor is called.
class Tracker {
public:
  Tracker() = default;
  void registerMetaData(MetaData::ValueBase NewMetaData);
  void clearMetaData();
  void writeToJSONDict(nlohmann::json &JSONNode) const;
  void writeToHDF5File(hdf5::node::Group RootNode) const;

private:
  mutable std::mutex MetaDataMutex;
  std::vector<std::shared_ptr<MetaDataInternal::ValueBaseInternal>>
      KnownMetaData;
};

} // namespace MetaData
