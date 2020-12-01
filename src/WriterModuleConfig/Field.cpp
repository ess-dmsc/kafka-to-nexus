// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Field.h"
#include "WriterModuleBase.h"

namespace WriterModuleConfig {
FieldBase::FieldBase(WriterModule::Base *Ptr, std::vector<std::string> const &Keys)
    : FieldKeys(Keys) {
  Ptr->addConfigField(this);
}

void FieldBase::makeRequired() { FieldRequired = true; }

} // namespace WriterModuleConfig
