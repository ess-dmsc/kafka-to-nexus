//
// Created by Jonas Nilsson on 2021-07-06.
//

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