// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ArrayGenerators.h"
#include "WriterRegistrar.h"
#include "helpers/HDFFileTestHelper.h"
#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>

using nlohmann::json;

namespace {
template <typename T>
std::vector<T> valuesOf(ArrayGenerators::GeneratedArray const &Array) {
  return std::get<std::vector<T>>(Array.Values);
}
} // namespace

// Expected values below are those given by numpy.

TEST(Arange, integerValuesExcludeStop) {
  auto Array = ArrayGenerators::arange(
      json::parse(R"({"name": "x", "start": 1, "stop": 10, "step": 1})"));
  EXPECT_EQ(Array.Name, "x");
  EXPECT_EQ(Array.Dimensions, Shape{9});
  EXPECT_EQ(valuesOf<int64_t>(Array),
            (std::vector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9}));
}

TEST(Arange, startAndStepHaveDefaults) {
  auto Array =
      ArrayGenerators::arange(json::parse(R"({"name": "x", "stop": 5})"));
  EXPECT_EQ(valuesOf<int64_t>(Array), (std::vector<int64_t>{0, 1, 2, 3, 4}));
}

TEST(Arange, negativeStep) {
  auto Array = ArrayGenerators::arange(
      json::parse(R"({"name": "x", "start": 10, "stop": 0, "step": -3})"));
  EXPECT_EQ(valuesOf<int64_t>(Array), (std::vector<int64_t>{10, 7, 4, 1}));
}

TEST(Arange, emptyWhenStopIsNotReached) {
  auto Array = ArrayGenerators::arange(
      json::parse(R"({"name": "x", "start": 5, "stop": 1})"));
  EXPECT_EQ(Array.Dimensions, Shape{0});
  EXPECT_TRUE(valuesOf<int64_t>(Array).empty());
}

TEST(Arange, anyFloatGivesFloatValues) {
  auto Array = ArrayGenerators::arange(
      json::parse(R"({"name": "x", "start": 0, "stop": 1, "step": 0.25})"));
  EXPECT_EQ(valuesOf<double>(Array),
            (std::vector<double>{0.0, 0.25, 0.5, 0.75}));
  Array = ArrayGenerators::arange(
      json::parse(R"({"name": "x", "start": 0, "stop": 1, "step": 0.1})"));
  EXPECT_EQ(Array.Dimensions, Shape{10});
}

TEST(Arange, invalidConfigThrows) {
  for (auto const &Config :
       {R"({"name": "x", "stop": 1, "step": 0})",
        R"({"name": "x", "stop": "ten"})", R"({"name": "x", "start": 1})",
        R"({"stop": 1})", R"({"name": "x", "stop": 1, "step": 1e-12})"}) {
    EXPECT_THROW(ArrayGenerators::arange(json::parse(Config)),
                 std::runtime_error)
        << Config;
  }
}

TEST(Linspace, includesEndpointByDefault) {
  auto Array = ArrayGenerators::linspace(
      json::parse(R"({"name": "x", "start": 0, "stop": 1, "num": 5})"));
  EXPECT_EQ(valuesOf<double>(Array),
            (std::vector<double>{0.0, 0.25, 0.5, 0.75, 1.0}));
}

TEST(Linspace, withoutEndpoint) {
  auto Array = ArrayGenerators::linspace(json::parse(
      R"({"name": "x", "start": 0, "stop": 1, "num": 5, "endpoint": false})"));
  // numpy, too, gives 0.6000000000000001 (3 * 0.2) and not 0.6
  EXPECT_EQ(valuesOf<double>(Array),
            (std::vector<double>{0.0, 0.2, 0.4, 3 * 0.2, 0.8}));
}

TEST(Linspace, defaultsToFiftyValues) {
  auto Array = ArrayGenerators::linspace(
      json::parse(R"({"name": "x", "start": 0, "stop": 1})"));
  EXPECT_EQ(Array.Dimensions, Shape{50});
  EXPECT_EQ(valuesOf<double>(Array).back(), 1.0);
}

TEST(Linspace, oneAndZeroValues) {
  auto Array = ArrayGenerators::linspace(
      json::parse(R"({"name": "x", "start": 2, "stop": 3, "num": 1})"));
  EXPECT_EQ(valuesOf<double>(Array), (std::vector<double>{2.0}));
  Array = ArrayGenerators::linspace(
      json::parse(R"({"name": "x", "start": 2, "stop": 3, "num": 0})"));
  EXPECT_TRUE(valuesOf<double>(Array).empty());
}

TEST(Linspace, invalidNumThrows) {
  for (auto const &Config :
       {R"({"name": "x", "start": 0, "stop": 1, "num": -1})",
        R"({"name": "x", "start": 0, "stop": 1, "num": 2.5})"}) {
    EXPECT_THROW(ArrayGenerators::linspace(json::parse(Config)),
                 std::runtime_error)
        << Config;
  }
}

class Meshgrid : public ::testing::Test {
public:
  json config(std::string const &Extra = "") {
    return json::parse(R"({"vectors": [
      {"module": "arange", "config": {"name": "x", "stop": 3}},
      {"module": "linspace", "config": {"name": "y", "start": 0, "stop": 1, "num": 2}}
    ])" + Extra + "}");
  }
};

TEST_F(Meshgrid, xyIndexingByDefault) {
  auto Arrays = ArrayGenerators::meshgrid(config());
  ASSERT_EQ(Arrays.size(), 2u);
  EXPECT_EQ(Arrays[0].Name, "x");
  EXPECT_EQ(Arrays[0].Dimensions, Shape({2, 3}));
  EXPECT_EQ(valuesOf<int64_t>(Arrays[0]),
            (std::vector<int64_t>{0, 1, 2, 0, 1, 2}));
  EXPECT_EQ(Arrays[1].Name, "y");
  EXPECT_EQ(Arrays[1].Dimensions, Shape({2, 3}));
  EXPECT_EQ(valuesOf<double>(Arrays[1]),
            (std::vector<double>{0, 0, 0, 1, 1, 1}));
}

TEST_F(Meshgrid, ijIndexing) {
  auto Arrays = ArrayGenerators::meshgrid(config(R"(, "indexing": "ij")"));
  EXPECT_EQ(Arrays[0].Dimensions, Shape({3, 2}));
  EXPECT_EQ(valuesOf<int64_t>(Arrays[0]),
            (std::vector<int64_t>{0, 0, 1, 1, 2, 2}));
  EXPECT_EQ(valuesOf<double>(Arrays[1]),
            (std::vector<double>{0, 1, 0, 1, 0, 1}));
}

TEST_F(Meshgrid, sparse) {
  auto Arrays = ArrayGenerators::meshgrid(config(R"(, "sparse": true)"));
  EXPECT_EQ(Arrays[0].Dimensions, Shape({1, 3}));
  EXPECT_EQ(valuesOf<int64_t>(Arrays[0]), (std::vector<int64_t>{0, 1, 2}));
  EXPECT_EQ(Arrays[1].Dimensions, Shape({2, 1}));
  EXPECT_EQ(valuesOf<double>(Arrays[1]), (std::vector<double>{0, 1}));
}

TEST_F(Meshgrid, threeDimensionsSwapOnlyTheFirstTwoAxes) {
  auto Arrays = ArrayGenerators::meshgrid(json::parse(R"({"vectors": [
      {"module": "arange", "config": {"name": "x", "stop": 2}},
      {"module": "arange", "config": {"name": "y", "stop": 3}},
      {"module": "arange", "config": {"name": "z", "stop": 4}}
    ]})"));
  for (auto const &Array : Arrays) {
    EXPECT_EQ(Array.Dimensions, Shape({3, 2, 4}));
  }
  auto First = [](auto const &Array) {
    auto Values = valuesOf<int64_t>(Array);
    return std::vector<int64_t>(Values.begin(), Values.begin() + 10);
  };
  EXPECT_EQ(First(Arrays[0]),
            (std::vector<int64_t>{0, 0, 0, 0, 1, 1, 1, 1, 0, 0}));
  EXPECT_EQ(First(Arrays[1]),
            (std::vector<int64_t>{0, 0, 0, 0, 0, 0, 0, 0, 1, 1}));
  EXPECT_EQ(First(Arrays[2]),
            (std::vector<int64_t>{0, 1, 2, 3, 0, 1, 2, 3, 0, 1}));
}

TEST_F(Meshgrid, invalidConfigThrows) {
  for (
      auto const &Config : {
          R"({"vectors": []})",
          R"({"vectors": [{"module": "arange", "config": {"name": "x", "stop": 2}}], "indexing": "yx"})",
          R"({"vectors": [{"module": "dataset", "config": {"name": "x", "values": [1]}}]})",
          R"({"vectors": [{"module": "meshgrid", "config": {"vectors": []}}]})",
          R"({"vectors": [{"module": "arange", "config": {"name": "x", "stop": 2}},
                           {"module": "arange", "config": {"name": "x", "stop": 3}}]})",
      }) {
    EXPECT_THROW(ArrayGenerators::meshgrid(json::parse(Config)),
                 std::runtime_error)
        << Config;
  }
}

TEST_F(Meshgrid, vectorAttributesFollowMeshgridAttributes) {
  auto Arrays = ArrayGenerators::generate("meshgrid", json::parse(R"({
      "config": {"vectors": [
        {"module": "arange", "config": {"name": "x", "stop": 2},
         "attributes": {"units": "mm"}},
        {"module": "arange", "config": {"name": "y", "stop": 2}}
      ]},
      "attributes": {"units": "m"}
    })"));
  EXPECT_EQ(Arrays[0].Attributes,
            (std::vector<json>{json{{"units", "m"}}, json{{"units", "mm"}}}));
  EXPECT_EQ(Arrays[1].Attributes, (std::vector<json>{json{{"units", "m"}}}));
}

class ArrayGeneratorsFile : public ::testing::Test {
public:
  void SetUp() override {
    TestFile = HDFFileTestHelper::createInMemoryTestFile("test-generators.nxs");
  }
  void init(std::string const &Children) {
    std::vector<ModuleHDFInfo> Modules;
    TestFile->init(R"({"children": [)" + Children + "]}", Modules, {}, true);
    EXPECT_TRUE(Modules.empty());
  }
  hdf5::node::Dataset dataset(std::string const &Path) {
    return hdf5::node::get_dataset(TestFile->hdfGroup(), Path);
  }
  bool exists(std::string const &Name) {
    return TestFile->hdfGroup().has_dataset(Name);
  }
  template <typename T> std::vector<T> read(std::string const &Path) {
    auto Dataset = dataset(Path);
    std::vector<T> Values(Dataset.dataspace().size());
    Dataset.read(Values);
    return Values;
  }
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> TestFile;
};

TEST_F(ArrayGeneratorsFile, arangeIsWritten) {
  init(R"({"module": "arange",
           "config": {"name": "x", "start": 1, "stop": 11},
           "attributes": {"units": "mm"}})");
  auto Dataset = dataset("/x");
  EXPECT_EQ(Dataset.datatype(), hdf5::datatype::create<int64_t>());
  EXPECT_EQ(read<int64_t>("/x"),
            (std::vector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
  std::string Units;
  Dataset.attributes["units"].read(Units,
                                   Dataset.attributes["units"].datatype());
  EXPECT_EQ(Units, "mm");
}

TEST_F(ArrayGeneratorsFile, linspaceIsWrittenWithRequestedType) {
  init(R"({"module": "linspace",
           "config": {"name": "x", "start": -1, "stop": 1, "num": 4, "dtype": "int32"}})");
  EXPECT_EQ(dataset("/x").datatype(), hdf5::datatype::create<int32_t>());
  EXPECT_EQ(read<int32_t>("/x"), (std::vector<int32_t>{-1, -1, 0, 1}));
}

TEST_F(ArrayGeneratorsFile, emptyArangeIsWritten) {
  init(
      R"({"module": "arange", "config": {"name": "x", "start": 1, "stop": 1}})");
  EXPECT_EQ(dataset("/x").dataspace().size(), 0);
}

TEST_F(ArrayGeneratorsFile, meshgridIsWritten) {
  init(R"({"module": "meshgrid", "config": {"vectors": [
            {"module": "arange", "config": {"name": "x", "stop": 3, "dtype": "float"}},
            {"module": "linspace", "config": {"name": "y", "start": 0, "stop": 1, "num": 2}}
          ]}})");
  auto Dimensions =
      hdf5::dataspace::Simple(dataset("/x").dataspace()).current_dimensions();
  EXPECT_EQ(Dimensions, hdf5::Dimensions({2, 3}));
  EXPECT_EQ(dataset("/x").datatype(), hdf5::datatype::create<float>());
  EXPECT_EQ(read<float>("/x"), (std::vector<float>{0, 1, 2, 0, 1, 2}));
  EXPECT_EQ(read<double>("/y"), (std::vector<double>{0, 0, 0, 1, 1, 1}));
}

TEST_F(ArrayGeneratorsFile, outOfRangeValuesAreNotWritten) {
  init(R"({"module": "arange",
           "config": {"name": "x", "start": 250, "stop": 260, "dtype": "uint8"}})");
  EXPECT_FALSE(exists("x"));
}

TEST_F(ArrayGeneratorsFile, unsupportedTypeIsNotWritten) {
  init(R"({"module": "arange",
           "config": {"name": "x", "stop": 3, "dtype": "string"}})");
  EXPECT_FALSE(exists("x"));
}

TEST_F(ArrayGeneratorsFile, invalidConfigDoesNotStopOtherDatasets) {
  init(R"({"module": "arange", "config": {"name": "bad", "stop": 3, "step": 0}},
          {"module": "arange", "config": {"name": "good", "stop": 3}})");
  EXPECT_FALSE(exists("bad"));
  EXPECT_EQ(read<int64_t>("/good"), (std::vector<int64_t>{0, 1, 2}));
}

TEST(ArrayGeneratorNames, areReservedForWriterModules) {
  for (auto const &Name : {"arange", "linspace", "meshgrid"}) {
    EXPECT_THROW(WriterModule::Registry::addWriterModule(
                     {"abcd", Name}, []() { return nullptr; }),
                 std::runtime_error)
        << Name;
  }
}
