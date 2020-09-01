// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "StreamHDFInfo.h"
#include "json.h"
#include "logger.h"
#include <deque>
#include <h5cpp/hdf5.hpp>
#include <string>

namespace HDFOperations {

bool findType(nlohmann::basic_json<> Attribute, std::string &DType);

void writeAttributes(hdf5::node::Node const &Node, nlohmann::json const *Value,
                     SharedLogger const &Logger);

void writeStringAttribute(hdf5::node::Node const &Node, std::string const &Name,
                          std::string const &Value);

void createHDFStructures(
    const nlohmann::json *Value, hdf5::node::Group const &Parent,
    uint16_t Level,
    hdf5::property::LinkCreationList const &LinkCreationPropertyList,
    hdf5::datatype::String const &FixedStringHDFType,
    std::vector<StreamHDFInfo> &HDFStreamInfo, std::deque<std::string> &Path,
    SharedLogger const &Logger);

void writeHDFISO8601AttributeCurrentTime(hdf5::node::Node const &Node,
                                         const std::string &Name);

void writeAttributesIfPresent(hdf5::node::Node const &Node,
                              nlohmann::json const &Values,
                              SharedLogger const &Logger);

void writeStringDataset(
    hdf5::node::Group const &Parent, const std::string &Name,
    hdf5::property::DatasetCreationList &DatasetCreationList,
    hdf5::dataspace::Dataspace &Dataspace, nlohmann::json const &Values);

void writeFixedSizeStringDataset(
    hdf5::node::Group const &Parent, const std::string &Name,
    hdf5::property::DatasetCreationList &DatasetCreationList,
    hdf5::dataspace::Dataspace &Dataspace, hsize_t ElementSize,
    const nlohmann::json *Values, SharedLogger const &Logger);

void writeGenericDataset(const std::string &DataType,
                         hdf5::node::Group const &Parent,
                         const std::string &Name,
                         const std::vector<hsize_t> &Sizes,
                         const std::vector<hsize_t> &Max, hsize_t ElementSize,
                         const nlohmann::json *Values,
                         SharedLogger const &Logger);

void writeDataset(hdf5::node::Group const &Parent, const nlohmann::json *Values,
                  SharedLogger const &Logger);

void writeObjectOfAttributes(hdf5::node::Node const &Node,
                             const nlohmann::json &Values);

void writeArrayOfAttributes(hdf5::node::Node const &Node,
                            const nlohmann::json &ValuesJson,
                            SharedLogger const &Logger);

void writeScalarAttribute(hdf5::node::Node const &Node, const std::string &Name,
                          const nlohmann::json &Values);

void writeAttrOfSpecifiedType(std::string const &DType,
                              hdf5::node::Node const &Node,
                              std::string const &Name, uint32_t StringSize,
                              hdf5::datatype::CharacterEncoding Encoding,
                              nlohmann::json const &Values,
                              SharedLogger const &Logger);

void addLinks(hdf5::node::Group const &Group, nlohmann::json const &Json,
              SharedLogger Logger);

} // namespace HDFOperations
