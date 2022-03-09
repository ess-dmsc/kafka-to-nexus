// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Dimension.h"
#include "Exceptions.h"

namespace WriterModule {
namespace hs00 {

template <typename EdgeType>
Dimension<EdgeType> Dimension<EdgeType>::createFromJson(json const &Json) {
  if (!Json.is_object()) {
    throw UnexpectedJsonInput(R"("shape" value is not an object.)");
  }
  Dimension Dim;
  try {
    Dim.Size = Json.at("size");
    Dim.Unit = Json.at("unit");
    Dim.Label = Json.at("label");
    Dim.DatasetName = Json.at("dataset_name");
    auto const &JsonEdges = Json.at("edges");
    if (!JsonEdges.is_array()) {
      throw UnexpectedJsonInput(R"("edges" is not an array.)");
    }
    if (JsonEdges.size() == size_t(-1) || JsonEdges.size() != Dim.Size + 1) {
      throw UnexpectedJsonInput();
    }
    for (auto const &E : JsonEdges) {
      Dim.Edges.push_back(E);
    }
  } catch (json::out_of_range const &e) {
    std::throw_with_nested(UnexpectedJsonInput());
  }
  return Dim;
}

template <typename EdgeType> size_t Dimension<EdgeType>::getSize() const {
  return Size;
}

template <typename EdgeType> std::string Dimension<EdgeType>::getLabel() const {
  return Label;
}

template <typename EdgeType> std::string Dimension<EdgeType>::getUnit() const {
  return Unit;
}

template <typename EdgeType>
std::vector<EdgeType> const &Dimension<EdgeType>::getEdges() const {
  return Edges;
}

template <typename EdgeType>
std::string Dimension<EdgeType>::getDatasetName() const {
  return DatasetName;
}

template size_t Dimension<uint32_t>::getSize() const;
template size_t Dimension<uint64_t>::getSize() const;
template size_t Dimension<double>::getSize() const;
template size_t Dimension<float>::getSize() const;

template std::string Dimension<uint32_t>::getLabel() const;
template std::string Dimension<uint64_t>::getLabel() const;
template std::string Dimension<double>::getLabel() const;
template std::string Dimension<float>::getLabel() const;

template std::string Dimension<uint32_t>::getUnit() const;
template std::string Dimension<uint64_t>::getUnit() const;
template std::string Dimension<double>::getUnit() const;
template std::string Dimension<float>::getUnit() const;

template std::vector<uint32_t> const &Dimension<uint32_t>::getEdges() const;
template std::vector<uint64_t> const &Dimension<uint64_t>::getEdges() const;
template std::vector<double> const &Dimension<double>::getEdges() const;
template std::vector<float> const &Dimension<float>::getEdges() const;

template Dimension<uint32_t>
Dimension<uint32_t>::createFromJson(json const &Json);
template Dimension<uint64_t>
Dimension<uint64_t>::createFromJson(json const &Json);
template Dimension<double> Dimension<double>::createFromJson(json const &Json);
template Dimension<float> Dimension<float>::createFromJson(json const &Json);

template std::string Dimension<uint32_t>::getDatasetName() const;
template std::string Dimension<uint64_t>::getDatasetName() const;
template std::string Dimension<double>::getDatasetName() const;
template std::string Dimension<float>::getDatasetName() const;

} // namespace hs00
} // namespace WriterModule
