#pragma once
#include "Value.h"
#include <memory>

namespace MetaData {

class Tracker {
public:
  Tracker() = default;
  void registerMetaData(MetaData::ValueBase NewMetaData);
  void clearMetaData();
  void writeToJSONDict(nlohmann::json &JSONNode) const;

private:
  std::vector<std::shared_ptr<MetaDataInternal::ValueBaseInternal>>
      KnownMetaData;
};

} // namespace MetaData