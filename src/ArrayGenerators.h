// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file ArrayGenerators.h
/// \brief Static datasets whose values are generated from a few parameters.
///
/// These are prototypes of possible NeXus "array generator" classes. Each
/// follows the semantics of the numpy function of the same name, and is
/// written to the file as a plain (materialised) dataset.

#pragma once

#include "MultiVector.h"
#include "json.h"
#include <h5cpp/hdf5.hpp>
#include <string>
#include <variant>
#include <vector>

namespace ArrayGenerators {

/// \brief Largest number of elements a single generated dataset may have.
///
/// Guards against a typo in a run start message (e.g. a tiny step) producing
/// a huge dataset while the file is being created.
constexpr size_t MaxElements{size_t{1} << 27};

/// \brief A generated array, ready to be written to a dataset.
struct GeneratedArray {
  std::string Name;
  std::string DataType;
  Shape Dimensions;
  /// Values in row-major order; int64 when generated from integers only.
  std::variant<std::vector<int64_t>, std::vector<double>> Values;
  /// "attributes" values to write to the dataset, applied in order so that
  /// later ones replace earlier attributes with the same name.
  std::vector<nlohmann::json> Attributes;
};

/// \brief True if the module name is that of an array generator.
bool isGenerator(std::string const &ModuleName);

/// \brief numpy.arange: values from start (default 0) up to, but excluding,
/// stop in increments of step (default 1).
///
/// The data type defaults to int64 when start, stop and step are all
/// integers, otherwise to double.
GeneratedArray arange(nlohmann::json const &Config);

/// \brief numpy.linspace: num (default 50) evenly spaced values from start to
/// stop, including stop unless endpoint is false.
GeneratedArray linspace(nlohmann::json const &Config);

/// \brief An N-dimensional array whose values are a linear function of their
/// indices: value[i0, i1, ...] = start + sum(strides[k] * ik).
///
/// Equivalent to numpy.lib.stride_tricks.as_strided applied to an arange, but
/// with strides in values rather than bytes. The data type defaults to int64
/// when start and all strides are integers, otherwise to double.
GeneratedArray strided(nlohmann::json const &Config);

/// \brief numpy.meshgrid: one N-dimensional array per input vector.
///
/// The vectors are given as a list of arange or linspace nodes, e.g.
/// {"module": "arange", "config": {...}, "attributes": {...}}. Each output
/// takes its name, data type and attributes from its vector. As for numpy,
/// indexing is "xy" (default) or "ij", and sparse (default false) gives
/// outputs with length 1 along all but their own axis.
std::vector<GeneratedArray> meshgrid(nlohmann::json const &Config);

/// \brief Generate the array(s) defined by a generator module.
///
/// \param ModuleName One of the names for which isGenerator is true.
/// \param Node The full JSON node, i.e. with "config" and "attributes".
std::vector<GeneratedArray> generate(std::string const &ModuleName,
                                     nlohmann::json const &Node);

/// \brief Write the dataset(s) defined by a generator module node.
///
/// Existing datasets (e.g. from a template file) are not overwritten, but have
/// their attributes updated, as for static datasets.
void writeGeneratedDatasets(hdf5::node::Group const &Parent,
                            std::string const &ModuleName,
                            nlohmann::json const &Node);

} // namespace ArrayGenerators
