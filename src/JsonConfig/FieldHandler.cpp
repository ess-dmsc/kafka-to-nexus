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
      Logger::Info(
          R"(Replacing the config field (key) "{}". Note: this is programming error (i.e. a bug) that should be fixed post-haste.)",
          Key);
    }
    FieldMap[Key] = Ptr;
  }
}

void FieldHandler::processConfigData(std::string const &ConfigJsonStr) {
  processConfigData(json::parse(ConfigJsonStr));
}

void FieldHandler::processConfigData(nlohmann::json const &JsonObj) {
  for (auto Iter = JsonObj.begin(); Iter != JsonObj.end(); ++Iter) {
    if (FieldMap.find(Iter.key()) == FieldMap.end()) {
      Logger::Error(R"(Json config field with name (key) "{}" is unknown.)",
                    Iter.key());
    } else {
      auto CurrentField = FieldMap.find(Iter.key());
      try {
        CurrentField->second->setValue(Iter.key(), Iter.value());
      } catch (json::type_error &E) {
        Logger::Error(
            R"(Got type error when trying to set json config field value (with key "{}"). The error message was: {})",
            Iter.key(), E.what());
      }
    }
  }
  std::set<FieldBase *> MissingFields;
  for (auto &Field : FieldMap) {
    if (Field.second->isRequired() && Field.second->hasDefaultValue()) {
      MissingFields.emplace(Field.second);
    }
  }
  if (!MissingFields.empty()) {
    int Ctr{1};
    auto ListOfKeys = std::accumulate(
        std::next(MissingFields.begin()), MissingFields.end(),
        fmt::format("{}. {}", Ctr, (*MissingFields.begin())->getKeys()),
        [&](auto a, auto b) {
          Ctr++;
          return a + fmt::format(" {}. {}", Ctr, b->getKeys());
        });
    throw std::runtime_error(
        "Missing (required) json config field(s) with key(s): " + ListOfKeys);
  }
}

} // namespace JsonConfig
