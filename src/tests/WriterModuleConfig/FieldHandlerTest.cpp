// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "WriterModuleConfig/Field.h"
#include "WriterModuleConfig/FieldHandler.h"

using namespace WriterModuleConfig;

class FieldStandIn : public FieldBase {
  FieldStandIn(std::string Key);
};

//TEST(FieldHandler, )