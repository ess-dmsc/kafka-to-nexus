#pragma once

#include "json.h"
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

template <typename EdgeType> class Shape {
private:
  using json = nlohmann::json;

public:
  static Shape createFromJson(json const &Json);

private:
  std::vector<EdgeType> edges;
};
}
}
}
