// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <stdexcept>

namespace Module {
namespace hs00 {

/// To signal errors in JSON command
class UnexpectedJsonInput : public std::runtime_error {
public:
  UnexpectedJsonInput() : std::runtime_error("UnexpectedJsonInput") {}
  explicit UnexpectedJsonInput(const std::string &Error)
      : std::runtime_error(Error) {}
};
} // namespace hs00
} // namespace Module
