// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Field.h"
#include "FieldHandler.h"

namespace JsonConfig {
FieldBase::FieldBase(FieldHandler *HandlerPtr,
                     std::vector<std::string> const &Keys)
    : FieldKeys(Keys) {
  HandlerPtr->registerField(this);
}

void FieldBase::makeRequired() { FieldRequired = true; }

} // namespace WriterModuleConfig
