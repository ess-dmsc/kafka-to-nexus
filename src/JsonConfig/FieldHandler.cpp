// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FieldHandler.h"
#include "Field.h"
#include "logger.h"
#include <nlohmann/json.hpp>
#include <set>

namespace JsonConfig {

using nlohmann::json;

void FieldHandler::registerField(FieldBase *Ptr) {
  auto Keys = Ptr->getKeys();
  for (auto const &Key : Keys) {
    if (FieldMap.find(Key) != FieldMap.end()) {
      LOG_WARN(
          "Replacing the config field (key) \"{}\". Note: this is programming "
          "error (i.e. a bug) that should be fixed post-haste.",
          Key);
    }
    FieldMap[Key] = Ptr;
  }
}

void FieldHandler::processConfigData(std::string const &ConfigJsonStr) {
  auto JsonObj = json::parse(ConfigJsonStr);
  for (auto Iter = JsonObj.begin(); Iter != JsonObj.end(); ++Iter) {
    if (FieldMap.find(Iter.key()) == FieldMap.end()) {
      LOG_ERROR("Writer module config field with name (key) \"{}\" is unknown. "
                "Is it a typo?",
                Iter.key());
    } else {
      auto CurrentField = FieldMap.find(Iter.key());
      try {
        CurrentField->second->setValue(Iter.value().dump());
      } catch (json::type_error &E) {
        LOG_ERROR("Got type error when trying to set writer module config "
                  "field value (with key \"{}\"). The error message was: {}",
                  Iter.key(), E.what());
      }
    }
  }
  std::set<FieldBase *> MissingFields;
  for (auto &Field : FieldMap) {
    if (Field.second->isRequried() and Field.second->hasDefaultValue()) {
      MissingFields.emplace(Field.second);
    }
  }
  if (not MissingFields.empty()) {
    int Ctr{1};
    auto ListOfKeys = std::accumulate(
        std::next(MissingFields.begin()), MissingFields.end(),
        fmt::format("{}. {}", Ctr, (*MissingFields.begin())->getKeys()),
        [&](auto a, auto b) {
          Ctr++;
          return a + fmt::format("{}. {}", Ctr, b->getKeys());
        });
    throw std::runtime_error(
        "Missing (requried) writer module config field(s) with key(s): " +
        ListOfKeys);
  }
}

} // namespace WriterModuleConfig
