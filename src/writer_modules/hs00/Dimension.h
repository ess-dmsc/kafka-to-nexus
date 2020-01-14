// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "json.h"
#include <string>
#include <vector>

namespace hs00 {

template <typename EdgeType> class Dimension {
private:
  using json = nlohmann::json;

public:
  static Dimension createFromJson(json const &Json);
  size_t getSize() const;
  std::string getLabel() const;
  std::string getUnit() const;
  std::vector<EdgeType> const &getEdges() const;
  std::string getDatasetName() const;

private:
  size_t Size = 0;
  std::string Label;
  std::string Unit;
  std::vector<EdgeType> Edges;
  std::string DatasetName;
};
} // namespace hs00
