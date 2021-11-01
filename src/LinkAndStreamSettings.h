// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "StreamHDFInfo.h"
#include "WriterModuleBase.h"
#include <string>

/// \brief Holder for the stream settings.
struct StreamSettings {
  StreamHDFInfo StreamHDFInfoObj;
  std::string Name;
  std::string Topic;
  std::string Module;
  std::string Source;
  std::string ConfigStreamJson;
  std::string Attributes;
  std::unique_ptr<WriterModule::Base> WriterModule;
  bool isLink = false;
};
