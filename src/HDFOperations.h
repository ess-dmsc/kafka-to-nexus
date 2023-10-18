// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "ModuleHDFInfo.h"
#include "ModuleSettings.h"
#include "MultiVector.h"
#include "json.h"
#include "logger.h"
#include <deque>
#include <h5cpp/hdf5.hpp>
#include <string>

namespace HDFOperations {

void writeAttributes(hdf5::node::Node const &Node, nlohmann::json const &Value);

void createHDFStructures(
    const nlohmann::json &Value, hdf5::node::Group const &Parent,
    uint16_t Level,
    hdf5::property::LinkCreationList const &LinkCreationPropertyList,
    hdf5::datatype::String const &FixedStringHDFType,
    std::vector<ModuleHDFInfo> &HDFStreamInfo, std::deque<std::string> &Path);

void writeHDFISO8601AttributeCurrentTime(hdf5::node::Node const &Node,
                                         const std::string &Name);

void writeAttributesIfPresent(hdf5::node::Node const &Node,
                              nlohmann::json const &Values);

void writeStringDataset(hdf5::node::Group const &Parent,
                        const std::string &Name,
                        MultiVector<std::string> const &Values);

void writeStringDatasetFromJson(hdf5::node::Group const &Parent,
                                const std::string &Name,
                                nlohmann::json const &Values);

void writeGenericDataset(const std::string &DataType,
                         hdf5::node::Group const &Parent,
                         const std::string &Name, nlohmann::json const &Values);

std::string writeDataset(hdf5::node::Group const &Parent,
                         nlohmann::json const &Values);

void writeObjectOfAttributes(hdf5::node::Node const &Node,
                             const nlohmann::json &Values);

void writeArrayOfAttributes(hdf5::node::Node const &Node,
                            const nlohmann::json &ValuesJson);

void writeScalarAttribute(hdf5::node::Node const &Node, const std::string &Name,
                          const nlohmann::json &Values);

void writeAttrOfSpecifiedType(std::string const &DType,
                              hdf5::node::Node const &Node,
                              std::string const &Name,
                              nlohmann::json const &Values);

void addLinks(hdf5::node::Group const &Group,
              std::vector<ModuleSettings> const &LinkSettingsList);
void addLinkToNode(hdf5::node::Group const &Group,
                   ModuleSettings const &LinkSettings);

Shape determineArrayDimensions(nlohmann::json const &Values);

template <typename T> T jsonElementConverter(nlohmann::json const &JsonObj) {
  return JsonObj.get<T>();
}

template <>
std::string jsonElementConverter<std::string>(nlohmann::json const &JsonObj);

template <typename T>
void populateMultiVector(nlohmann::json const &JsonObj,
                         MultiVector<T> &TargetVector, Shape CurrentPosition,
                         size_t CurrentLevel) {
  for (auto const &Element : JsonObj) {
    if (Element.is_array()) {
      populateMultiVector(Element, TargetVector, CurrentPosition,
                          CurrentLevel + 1);
    } else {
      TargetVector.at(CurrentPosition) = jsonElementConverter<T>(Element);
    }
    ++CurrentPosition[CurrentLevel];
  }
}

template <typename T>
MultiVector<T> jsonArrayToMultiArray(nlohmann::json const &ValueJson) {
  auto ArraySize = determineArrayDimensions(ValueJson);
  MultiVector<T> ReturnVector(ArraySize);
  populateMultiVector(ValueJson, ReturnVector, Shape(ArraySize.size()), 0);
  return ReturnVector;
}

} // namespace HDFOperations
