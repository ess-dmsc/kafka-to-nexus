// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Dimension.h"
#include "json.h"
#include <vector>

namespace Module {
namespace hs00 {

template <typename EdgeType> class Shape {
private:
  using json = nlohmann::json;

public:
  static Shape createFromJson(json const &Json);
  size_t getNDIM() const;
  std::vector<Dimension<EdgeType>> const &getDimensions() const;
  size_t getTotalItems() const;

private:
  std::vector<Dimension<EdgeType>> Dimensions;
};
} // namespace hs00
} // namespace Module
