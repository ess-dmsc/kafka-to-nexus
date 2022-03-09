// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "WriterModuleBase.h"
#include "WriterRegistrar.h"

namespace WriterModule {
Base::Base(bool AcceptRepeatedTimestamps, std::string_view const &NX_class,
           std::vector<std::string> ExtraModules)
    : WriteRepeatedTimestamps(AcceptRepeatedTimestamps), NX_class(NX_class) {
  for (auto &Module : ExtraModules) {
    try {
      auto FoundModule = WriterModule::Registry::find(Module);
      FoundExtraModules.push_back(FoundModule.second.Name);
      ExtraModuleEnabled[FoundModule.second.Name] =
          std::make_unique<JsonConfig::Field<bool>>(
              this, fmt::format("enable_{}", FoundModule.second.Name), true);
    } catch (std::out_of_range const &) {
      LOG_ERROR("Unable to add extra module \"{}\" to list (of extra modules) "
                "as it does not exist.",
                Module);
    }
  }
}
} // namespace WriterModule