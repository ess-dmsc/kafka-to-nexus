// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <flatbuffers/flatbuffers.h>
#include <memory>

namespace FileWriter {
namespace Schemas {
namespace f142 {

template <typename T> using uptr = std::unique_ptr<T>;

#include "f142_logdata_generated.h"

using FBUF = LogData;
} // namespace f142
} // namespace Schemas
} // namespace FileWriter
