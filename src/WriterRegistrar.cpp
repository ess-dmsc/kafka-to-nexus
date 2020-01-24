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
  auto const Factories = getFactories();
  for (auto const &Item : getFactories()) {
    ReturnMap.insert({Item.second.Id, Item.second.Name});
  }
  return ReturnMap;
}

ModuleFactory const &find(std::string const &FlatbufferID) {
  auto const &Factories = getFactories();
  auto FoundFactory = std::find_if(Factories.begin(), Factories.end(),
                                   [&FlatbufferID](auto const &CItem) {
                                     return CItem.second.Id == FlatbufferID;
                                   });
  if (FoundFactory == Factories.end()) {
    throw std::out_of_range("Unable to find factory with the id: " +
                            FlatbufferID);
  }
  return (*FoundFactory).second.FactoryPtr;
}

ModuleFactory const &find(std::string const &FlatbufferID,
                          std::string const &ModuleName) {
  auto const &Factories = getFactories();
  auto CurrentHash = getWriterModuleHash(FlatbufferID, ModuleName);
  auto FoundItem = Factories.find(CurrentHash);
  if (FoundItem != Factories.end()) {
    return FoundItem->second.FactoryPtr;
  }
  FoundItem = std::find_if(std::cbegin(Factories), std::cend(Factories),
                           [&ModuleName](auto const &CItem) {
                             return CItem.second.Name == ModuleName;
                           });
  if (FoundItem != Factories.end()) {
    if (not FlatbufferID.empty() and FoundItem->second.Id != FlatbufferID) {
      throw std::runtime_error(
          "The provided flatbuffer id (" + FlatbufferID +
          ") does not correspont to (" + FoundItem->second.Id +
          ") that of the found writer module (" + ModuleName + ").");
    }
    return FoundItem->second.FactoryPtr;
  }
  try {
    return find(FlatbufferID);
  } catch (std::out_of_range &) {
  }
  throw std::out_of_range("Unable to find module with name \" " + ModuleName +
                          "\"");
}

ModuleFactory const &find(WriterModuleHash ModuleHash) {
  return getFactories().at(ModuleHash).FactoryPtr;
}

void clear() { getFactories().clear(); }

// cppcheck-suppress unusedFunction
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
  Factories[ModuleHash] = {Value, FlatbufferID, ModuleName};
}
} // namespace Registry
} // namespace WriterModule
