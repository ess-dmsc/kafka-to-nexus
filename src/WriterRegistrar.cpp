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

WriterModuleHash getWriterModuleHash(ModuleFlatbufferID const &ID) {
  return std::hash<std::string>{}(ID.Id + ID.Name);
}

std::vector<ModuleFlatbufferID>
getFactoryIdsAndNames() {
  std::vector<ModuleFlatbufferID> ReturnList;
  std::transform(getFactories().cbegin(), getFactories().cend(),
                 std::back_inserter(ReturnList),
                 [](auto &Item) -> ModuleFlatbufferID {
                   return {Item.second.Id, Item.second.Name};
                 });
  return ReturnList;
}

FactoryAndID const find(std::string const &ModuleName) {
  auto const &Factories = getFactories();
  auto FoundItem = std::find_if(std::cbegin(Factories), std::cend(Factories),
                                [&ModuleName](auto const &CItem) {
                                  return CItem.second.Name == ModuleName;
                                });
  if (FoundItem == Factories.end()) {
    FoundItem = std::find_if(std::cbegin(Factories), std::cend(Factories),
                             [&ModuleName](auto const &CItem) {
      return CItem.second.Id == ModuleName;
    });
    if (FoundItem == Factories.end()) {
      throw std::out_of_range("Unable to find module with name/id \"" + ModuleName +
      "\"");
    }
  }
  return {FoundItem->second.FactoryPtr, {FoundItem->second.Id, FoundItem->second.Name}};
}

FactoryAndID const find(WriterModuleHash ModuleHash) {
  auto FoundModule = getFactories().at(ModuleHash);
  return {FoundModule.FactoryPtr, {FoundModule.Id, FoundModule.Name}};
}

void clear() { getFactories().clear(); }

void addWriterModule(ModuleFlatbufferID const &ID, ModuleFactory Value) {
  auto &Factories = getFactories();
  if (ID.Id.size() != 4) {
    throw std::runtime_error(
        "The number of characters in the Flatbuffer id string must be 4.");
  }
  if (ID.Name == "dataset") {
    throw std::runtime_error("The writer module name \"dataset\" has been "
                             "reserved and can not be used.");
  }
  auto ModuleHash = getWriterModuleHash(ID);
  if (Factories.find(ModuleHash) != Factories.end()) {
    auto s = fmt::format("Writer module with name \"{}\" that processes \"{}\" "
                         "flatbuffers already exists.",
                         ID.Name, ID.Id);
    throw std::runtime_error(s);
  }
  if (std::find_if(Factories.begin(), Factories.end(),
                   [&ID](auto const &CItem) {
                     return CItem.second.Name == ID.Name;
                   }) != Factories.end()) {
    auto s = fmt::format("Writer module with name \"{}\" already exists.",
                         ID.Name);
    throw std::runtime_error(s);
  }
  Factories[ModuleHash] = {std::move(Value), ID.Id, ID.Name};
}
} // namespace Registry
} // namespace WriterModule
