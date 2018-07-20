#pragma once

#include "json.h"
#include <string>
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename EdgeType> class Dimension {
private:
  using json = nlohmann::json;

public:
  Dimension createFromJson(json const &Json);
  size_t size() const;
  std::string label() const;
  std::string unit() const;
  std::vector<EdgeType> const &edges() const;
};
}
}
}
