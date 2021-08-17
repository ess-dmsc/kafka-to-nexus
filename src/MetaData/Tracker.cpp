// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Tracker.h"

namespace MetaData {

void Tracker::registerMetaData(MetaData::ValueBase NewMetaData) {
  KnownMetaData.emplace_back(NewMetaData.getValuePtr());
}
void Tracker::clearMetaData() { KnownMetaData.clear(); }

void Tracker::writeToJSONDict(nlohmann::json &JSONNode) const {
  for (auto const &MetaData : KnownMetaData) {
    auto JSONObj = MetaData->getAsJSON();
    JSONNode.insert(JSONObj.cbegin(), JSONObj.cend());
  }
}

} // namespace MetaData