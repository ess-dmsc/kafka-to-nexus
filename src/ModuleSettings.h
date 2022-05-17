// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "ModuleHDFInfo.h"
#include "WriterModuleBase.h"
#include <string>

/// \brief Holder for the stream settings.
struct ModuleSettings {
  ModuleHDFInfo ModuleHDFInfoObj;
  std::string Name;
  std::string Topic;
  std::string Module;
  std::string Source;
  std::string ConfigStreamJson{"{}"};
  std::string Attributes{""};
  std::unique_ptr<WriterModule::Base> WriterModule;
  bool isLink;

  /// \brief Get a copy of the instance of this object but without some fields.
  ///
  /// For use when instantiating an "extra" writer module. Thus has some fields
  /// not being copied over.
  ModuleSettings getCopyForExtraModule() const {
    ModuleSettings ReturnCopy;
    ReturnCopy.ModuleHDFInfoObj = ModuleHDFInfoObj;
    ReturnCopy.Name = Name;
    ReturnCopy.Topic = Topic;
    ReturnCopy.Source = Source;
    ReturnCopy.isLink = false;
    return ReturnCopy;
  }
};
