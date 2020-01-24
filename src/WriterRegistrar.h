// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \brief Keeps track of the registered FlatbufferReader instances.
///
/// Writer modules register themselves via instantiation of a
/// `WriterModule::Registry::Registrar`.

#pragma once
#include "WriterModuleBase.h"
#include <map>
#include <string>

namespace WriterModule {
namespace Registry {
using ModuleFactory = std::function<std::unique_ptr<WriterModule::Base>()>;
using WriterModuleHash = size_t;

WriterModuleHash getWriterModuleHash(std::string const &FlatbufferID,
                                     std::string const &ModuleName);

/// \brief Get all registered modules.
///
/// \return A reference to the map of registered modules.
std::map<std::string, std::string> getFactoryIdsAndNames();

/// \brief Registers a new writer module. Called by `Registrar`.
///
/// \param key
/// \param value
void addWriterModule(std::string const &FlatbufferID,
                     std::string const &ModuleName, ModuleFactory Value);

/// \brief Get `ModuleFactory for a given flatbuffer id.
///
/// \return Matching `ModuleFactory`.
ModuleFactory const &find(std::string const &FlatbufferID);

/// \brief Get module factory for a given flatbuffer id and/or module name.
/// \param[in] FlatbufferID A four character flatbuffer id. Will be ignored if
/// empty string.
/// \param[in] ModuleName Module name of module instantiated by
/// factory function.
/// \return A module factory.
/// \throw std::runtime_error if module name and flatbuffer id does not exist or
/// they do not match.
ModuleFactory const &find(std::string const &FlatbufferID,
                          std::string const &ModuleName);

ModuleFactory const &find(WriterModuleHash ModuleHash);

void clear();

/// \brief  Registers the writer module at program start if instantiated in the
/// namespace of each writer module with the writer module given as `Module`.
template <typename Module> class Registrar {
public:
  /// \brief Register the writer module given in template parameter `Module`
  /// under the
  /// identifier `FlatbufferID`.
  ///
  /// \param FlatbufferID The unique identifier for this writer module.
  explicit Registrar(std::string const &FlatbufferID,
                     std::string const &ModuleName) {
    auto FactoryFunction = []() { return std::make_unique<Module>(); };
    addWriterModule(FlatbufferID, ModuleName, FactoryFunction);
  };
};
} // namespace Registry
} // namespace WriterModule
