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
}
}
}
