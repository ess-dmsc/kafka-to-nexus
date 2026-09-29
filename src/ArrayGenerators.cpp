// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ArrayGenerators.h"
#include "HDFOperations.h"
#include "JsonConfig/Field.h"
#include "JsonConfig/FieldHandler.h"
#include "logger.h"
#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <map>
#include <numeric>
#include <set>

namespace ArrayGenerators {
using nlohmann::json;

namespace {

class ArangeConfig : public JsonConfig::FieldHandler {
public:
  explicit ArangeConfig(json const &JsonObj) { processConfigData(JsonObj); }
  JsonConfig::RequiredField<std::string> Name{this, "name"};
  JsonConfig::Field<json> Start{this, "start", 0};
  JsonConfig::RequiredField<json> Stop{this, "stop"};
  JsonConfig::Field<json> Step{this, "step", 1};
  JsonConfig::Field<std::string> DataType{this, {"dtype", "type"}, ""};
};

class LinspaceConfig : public JsonConfig::FieldHandler {
public:
  explicit LinspaceConfig(json const &JsonObj) { processConfigData(JsonObj); }
  JsonConfig::RequiredField<std::string> Name{this, "name"};
  JsonConfig::RequiredField<json> Start{this, "start"};
  JsonConfig::RequiredField<json> Stop{this, "stop"};
  JsonConfig::Field<json> Num{this, "num", 50};
  JsonConfig::Field<bool> Endpoint{this, "endpoint", true};
  JsonConfig::Field<std::string> DataType{this, {"dtype", "type"}, ""};
};

class MeshgridConfig : public JsonConfig::FieldHandler {
public:
  explicit MeshgridConfig(json const &JsonObj) { processConfigData(JsonObj); }
  JsonConfig::RequiredField<json> Vectors{this, "vectors"};
  JsonConfig::Field<std::string> Indexing{this, "indexing", "xy"};
  JsonConfig::Field<bool> Sparse{this, "sparse", false};
};

std::string const Arange{"arange"};
std::string const Linspace{"linspace"};
std::string const Meshgrid{"meshgrid"};

double toFiniteDouble(json const &Value, std::string const &Key) {
  if (!Value.is_number()) {
    throw std::runtime_error(
        fmt::format(R"("{}" must be a number, not {})", Key, Value.dump()));
  }
  auto const Result = Value.get<double>();
  if (!std::isfinite(Result)) {
    throw std::runtime_error(fmt::format(R"("{}" must be finite)", Key));
  }
  return Result;
}

void checkElementCount(double Count) {
  if (Count > static_cast<double>(MaxElements)) {
    throw std::runtime_error(
        fmt::format("Generated array would have {} elements, more than the "
                    "maximum of {}.",
                    Count, MaxElements));
  }
}

/// Integer arange, following numpy in producing int64 values.
std::vector<int64_t> integerArange(int64_t Start, int64_t Stop, int64_t Step) {
  // Work with unsigned differences to avoid overflowing for extreme limits.
  uint64_t Count{0};
  if (Step > 0 && Stop > Start) {
    Count = (static_cast<uint64_t>(Stop) - static_cast<uint64_t>(Start) - 1) /
                static_cast<uint64_t>(Step) +
            1;
  } else if (Step < 0 && Stop < Start) {
    Count = (static_cast<uint64_t>(Start) - static_cast<uint64_t>(Stop) - 1) /
                (uint64_t{0} - static_cast<uint64_t>(Step)) +
            1;
  }
  checkElementCount(static_cast<double>(Count));
  std::vector<int64_t> Values(Count);
  for (size_t i = 0; i < Count; ++i) {
    Values[i] = Start + static_cast<int64_t>(i) * Step;
  }
  return Values;
}

std::vector<double> floatArange(double Start, double Stop, double Step) {
  auto const Count = std::max(0.0, std::ceil((Stop - Start) / Step));
  checkElementCount(Count);
  std::vector<double> Values(static_cast<size_t>(Count));
  for (size_t i = 0; i < Values.size(); ++i) {
    Values[i] = Start + static_cast<double>(i) * Step;
  }
  return Values;
}

size_t length(GeneratedArray const &Array) {
  return std::visit([](auto const &Values) { return Values.size(); },
                    Array.Values);
}

/// Repeat a 1-D vector over Dimensions, varying along Axis only.
template <typename T>
std::vector<T> broadcast(std::vector<T> const &Vector, Shape const &Dimensions,
                         size_t Axis) {
  size_t Stride{1};
  for (size_t i = Axis + 1; i < Dimensions.size(); ++i) {
    Stride *= Dimensions[i];
  }
  std::vector<T> Result(extentToSize(Dimensions));
  for (size_t i = 0; i < Result.size(); ++i) {
    Result[i] = Vector[(i / Stride) % Dimensions[Axis]];
  }
  return Result;
}

/// Convert a generated value to the dataset's type, flooring (as numpy does
/// for linspace) when the target type is an integer.
template <typename DT, typename VT> DT convertValue(VT Value) {
  if constexpr (std::is_integral_v<DT>) {
    auto Floored = std::floor(static_cast<double>(Value));
    auto const Limit =
        std::ldexp(1.0, std::numeric_limits<DT>::digits); // exclusive
    auto const Lowest = std::is_signed_v<DT> ? -Limit : 0.0;
    if (Floored < Lowest || Floored >= Limit) {
      throw std::runtime_error(fmt::format(
          "Generated value {} is out of range for the requested dtype.",
          Value));
    }
    if constexpr (std::is_integral_v<VT>) {
      return static_cast<DT>(Value);
    } else {
      return static_cast<DT>(Floored);
    }
  } else {
    return static_cast<DT>(Value);
  }
}

template <typename DT>
void writeArray(hdf5::node::Group const &Parent, GeneratedArray const &Array) {
  MultiVector<DT> Data(Array.Dimensions);
  std::visit(
      [&Data](auto const &Values) {
        std::transform(Values.cbegin(), Values.cend(), Data.Data.begin(),
                       [](auto Value) { return convertValue<DT>(Value); });
      },
      Array.Values);
  auto const &Dims = Array.Dimensions;
  auto DataSpace =
      hdf5::dataspace::Simple(hdf5::Dimensions(Dims.begin(), Dims.end()));
  auto Dataset = Parent.create_dataset(Array.Name, hdf5::datatype::create<DT>(),
                                       DataSpace);
  if (Data.size() > 0) {
    Dataset.write(Data);
  }
}

void writeArrayOfType(hdf5::node::Group const &Parent,
                      GeneratedArray const &Array) {
  auto DataType = Array.DataType;
  if (DataType.empty()) {
    DataType = std::holds_alternative<std::vector<int64_t>>(Array.Values)
                   ? "int64"
                   : "double";
  }
  std::map<std::string, std::function<void()>> WriteMap{
      {"uint8", [&]() { writeArray<uint8_t>(Parent, Array); }},
      {"uint16", [&]() { writeArray<uint16_t>(Parent, Array); }},
      {"uint32", [&]() { writeArray<uint32_t>(Parent, Array); }},
      {"uint64", [&]() { writeArray<uint64_t>(Parent, Array); }},
      {"int8", [&]() { writeArray<int8_t>(Parent, Array); }},
      {"int16", [&]() { writeArray<int16_t>(Parent, Array); }},
      {"int32", [&]() { writeArray<int32_t>(Parent, Array); }},
      {"int", [&]() { writeArray<int32_t>(Parent, Array); }},
      {"int64", [&]() { writeArray<int64_t>(Parent, Array); }},
      {"float", [&]() { writeArray<float>(Parent, Array); }},
      {"double", [&]() { writeArray<double>(Parent, Array); }},
  };
  auto Writer = WriteMap.find(DataType);
  if (Writer == WriteMap.end()) {
    throw std::runtime_error(
        fmt::format(R"(Unsupported dtype "{}" for generated dataset "{}".)",
                    DataType, Array.Name));
  }
  Writer->second();
}

json attributesOf(json const &Node) {
  if (auto Found = Node.find("attributes"); Found != Node.end()) {
    return *Found;
  }
  return json();
}

json configOf(json const &Node) {
  if (auto Found = Node.find("config"); Found != Node.end()) {
    return *Found;
  }
  throw std::runtime_error(R"(Array generator is missing its "config".)");
}

} // namespace

bool isGenerator(std::string const &ModuleName) {
  return ModuleName == Arange || ModuleName == Linspace ||
         ModuleName == Meshgrid;
}

GeneratedArray arange(json const &Config) {
  ArangeConfig Parsed(Config);
  auto const &Start = Parsed.Start.get_value();
  auto const &Stop = Parsed.Stop.get_value();
  auto const &Step = Parsed.Step.get_value();
  auto const StepValue = toFiniteDouble(Step, "step");
  toFiniteDouble(Start, "start");
  toFiniteDouble(Stop, "stop");
  if (StepValue == 0) {
    throw std::runtime_error(R"(arange "step" must not be zero.)");
  }
  GeneratedArray Result{Parsed.Name, Parsed.DataType, {}, {}, {}};
  if (Start.is_number_integer() && Stop.is_number_integer() &&
      Step.is_number_integer()) {
    Result.Values = integerArange(Start.get<int64_t>(), Stop.get<int64_t>(),
                                  Step.get<int64_t>());
  } else {
    Result.Values =
        floatArange(Start.get<double>(), Stop.get<double>(), StepValue);
  }
  Result.Dimensions = {length(Result)};
  return Result;
}

GeneratedArray linspace(json const &Config) {
  LinspaceConfig Parsed(Config);
  auto const Start = toFiniteDouble(Parsed.Start.get_value(), "start");
  auto const Stop = toFiniteDouble(Parsed.Stop.get_value(), "stop");
  auto const &Num = Parsed.Num.get_value();
  if (!Num.is_number_integer() || Num.get<int64_t>() < 0) {
    throw std::runtime_error(
        fmt::format(R"(linspace "num" must be a non-negative integer, not {})",
                    Num.dump()));
  }
  auto const Count = Num.get<uint64_t>();
  checkElementCount(static_cast<double>(Count));
  bool const Endpoint = Parsed.Endpoint;
  auto const Divisions = Endpoint && Count > 0 ? Count - 1 : Count;

  std::vector<double> Values(Count);
  auto const Step = Divisions > 0 ? (Stop - Start) / Divisions : 0.0;
  for (size_t i = 0; i < Count; ++i) {
    Values[i] = Start + static_cast<double>(i) * Step;
  }
  if (Endpoint && Count > 1) {
    Values.back() = Stop;
  }
  return {Parsed.Name, Parsed.DataType, {Count}, std::move(Values), {}};
}

std::vector<GeneratedArray> meshgrid(json const &Config) {
  MeshgridConfig Parsed(Config);
  auto const &VectorNodes = Parsed.Vectors.get_value();
  if (!VectorNodes.is_array() || VectorNodes.empty()) {
    throw std::runtime_error(
        R"(meshgrid "vectors" must be a non-empty list of arange or linspace nodes.)");
  }
  auto const Indexing = Parsed.Indexing.get_value();
  if (Indexing != "xy" && Indexing != "ij") {
    throw std::runtime_error(fmt::format(
        R"(meshgrid "indexing" must be "xy" or "ij", not "{}")", Indexing));
  }

  std::vector<GeneratedArray> Vectors;
  std::set<std::string> Names;
  for (auto const &Node : VectorNodes) {
    auto const Module = Node.value("module", "");
    if (Module != Arange && Module != Linspace) {
      throw std::runtime_error(fmt::format(
          R"(meshgrid vectors must be "arange" or "linspace" modules, not "{}")",
          Module));
    }
    auto Vector = generate(Module, Node).front();
    if (!Names.insert(Vector.Name).second) {
      throw std::runtime_error(fmt::format(
          R"(meshgrid vector name "{}" is used more than once.)", Vector.Name));
    }
    Vectors.push_back(std::move(Vector));
  }

  // numpy: "xy" indexing swaps the first two output axes.
  auto const Rank = Vectors.size();
  std::vector<size_t> Axes(Rank);
  std::iota(Axes.begin(), Axes.end(), 0);
  if (Indexing == "xy" && Rank > 1) {
    std::swap(Axes[0], Axes[1]);
  }
  Shape FullDimensions(Rank);
  double Elements{1};
  for (size_t i = 0; i < Rank; ++i) {
    FullDimensions[Axes[i]] = length(Vectors[i]);
    Elements *= static_cast<double>(length(Vectors[i]));
  }
  if (!Parsed.Sparse) {
    checkElementCount(Elements);
  }

  for (size_t i = 0; i < Rank; ++i) {
    auto &Vector = Vectors[i];
    Shape Dimensions(Rank, 1);
    Dimensions[Axes[i]] = length(Vector);
    if (!Parsed.Sparse) {
      Dimensions = FullDimensions;
    }
    Vector.Values = std::visit(
        [&](auto const &Values) -> decltype(GeneratedArray::Values) {
          return broadcast(Values, Dimensions, Axes[i]);
        },
        Vector.Values);
    Vector.Dimensions = Dimensions;
  }
  return Vectors;
}

std::vector<GeneratedArray> generate(std::string const &ModuleName,
                                     json const &Node) {
  auto const Config = configOf(Node);
  auto const Attributes = attributesOf(Node);
  std::vector<GeneratedArray> Arrays;
  if (ModuleName == Arange) {
    Arrays.push_back(arange(Config));
  } else if (ModuleName == Linspace) {
    Arrays.push_back(linspace(Config));
  } else if (ModuleName == Meshgrid) {
    Arrays = meshgrid(Config);
  } else {
    throw std::runtime_error(
        fmt::format(R"("{}" is not an array generator.)", ModuleName));
  }
  if (!Attributes.is_null()) {
    // Attributes of the outer node apply first, so that those given for a
    // meshgrid vector take precedence over those given for the meshgrid.
    for (auto &Array : Arrays) {
      Array.Attributes.insert(Array.Attributes.begin(), Attributes);
    }
  }
  return Arrays;
}

void writeGeneratedDatasets(hdf5::node::Group const &Parent,
                            std::string const &ModuleName, json const &Node) {
  for (auto const &Array : generate(ModuleName, Node)) {
    if (Parent.has_dataset(Array.Name)) {
      Logger::Debug("Dataset {} already exists in group {}", Array.Name,
                    std::string(Parent.link().path()));
    } else {
      Logger::Debug("Creating {} dataset {} in group {}", ModuleName,
                    Array.Name, std::string(Parent.link().path()));
      writeArrayOfType(Parent, Array);
    }
    auto Dataset = Parent.get_dataset(Array.Name);
    for (auto const &Attributes : Array.Attributes) {
      HDFOperations::writeAttributes(Dataset, Attributes);
    }
  }
}

} // namespace ArrayGenerators
