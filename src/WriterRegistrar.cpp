// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "WriterRegistrar.h"

namespace WriterModule {
namespace Registry {

struct FactoryInfo {
  ModuleFactory FactoryPtr;
  std::string Id;
  std::string Name;
};

std::map<WriterModuleHash, FactoryInfo> &getFactories() {
  static std::map<WriterModuleHash, FactoryInfo> Factories;
  return Factories;
}

WriterModuleHash getWriterModuleHash(std::string const &FlatbufferID,
                                     std::string const &ModuleName) {
  return std::hash<std::string>{}(FlatbufferID + ModuleName);
}

std::map<std::string, std::string> getFactoryIdsAndNames() {
  std::map<std::string, std::string> ReturnMap;
  for (auto const &Item : getFactories()) {
    ReturnMap.insert({Item.second.Id, Item.second.Name});
  }
  return ReturnMap;
}

std::pair<ModuleFactory, std::string> const
find(std::string const &ModuleName) {
  auto const &Factories = getFactories();
  auto FoundItem = std::find_if(std::cbegin(Factories), std::cend(Factories),
                                [&ModuleName](auto const &CItem) {
                                  return CItem.second.Name == ModuleName;
                                });
  if (FoundItem == Factories.end()) {
    throw std::out_of_range("Unable to find module with name \" " + ModuleName +
                            "\"");
  }
  return {FoundItem->second.FactoryPtr, FoundItem->second.Id};
}

std::pair<ModuleFactory, std::string> const find(WriterModuleHash ModuleHash) {
  auto FoundModule = getFactories().at(ModuleHash);
  return {FoundModule.FactoryPtr, FoundModule.Id};
}

void clear() { getFactories().clear(); }

void addWriterModule(std::string const &FlatbufferID,
                     std::string const &ModuleName, ModuleFactory Value) {
  auto &Factories = getFactories();
  if (FlatbufferID.size() != 4) {
    throw std::runtime_error(
        "The number of characters in the Flatbuffer id string must be 4.");
  }
  auto ModuleHash = getWriterModuleHash(FlatbufferID, ModuleName);
  if (Factories.find(ModuleHash) != Factories.end()) {
    auto s = fmt::format("Writer module with name \"{}\" that processes \"{}\" "
                         "flatbuffers already exists.",
                         ModuleName, FlatbufferID);
    throw std::runtime_error(s);
  }
  if (std::find_if(Factories.begin(), Factories.end(),
                   [&ModuleName](auto const &CItem) {
                     return CItem.second.Name == ModuleName;
                   }) != Factories.end()) {
    auto s = fmt::format("Writer module with name \"{}\" already exists.",
                         ModuleName, FlatbufferID);
    throw std::runtime_error(s);
  }
  Factories[ModuleHash] = {std::move(Value), FlatbufferID, ModuleName};
}
} // namespace Registry
} // namespace WriterModule
