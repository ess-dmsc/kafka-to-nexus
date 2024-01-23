#include <da00_dataarray_generated.h>
#include <algorithm>
#include <cmath>
#include <fstream>
#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>

#include "FlatbufferReader.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"

#include "WriterRegistrar.h"

#include "AccessMessageMetadata/da00/da00_Extractor.h"
#include "WriterModule/da00/da00_Writer.h"
#include "WriterModule/da00/da00_Variable.h"


::testing::AssertionResult DatasetTestFailed(std::string_view name, std::string_view message) {
  return ::testing::AssertionFailure() << "Dataset " << name << " " << message;
}

::testing::AssertionResult NodeHasAttribue(
  hdf5::node::Group const &group, std::string const &name) {
  if (!group.attributes.exists(name))
    return DatasetTestFailed(group.link().path().operator std::string(), "has no attribute " + name);
  return ::testing::AssertionSuccess();
}
template<class T=void>
::testing::AssertionResult DatasetIsValid(
  hdf5::node::Group const &group,
  const std::string & name) {
  if (!group.has_dataset(name)) return DatasetTestFailed(name, "not found");
  const auto dataset = group.get_dataset(name);
  if (!dataset.dataspace().is_valid()) return DatasetTestFailed(name, "is not valid");
  return ::testing::AssertionSuccess();
}


template<class T=void>
::testing::AssertionResult DatasetHasLabel(
  hdf5::node::Group const &group,
  const std::string & name,
  std::string_view label) {
  auto dataset = group.get_dataset(name);
  if (!dataset.attributes.exists("label")) return DatasetTestFailed(name, "has no label attribute");
  std::string value;
  dataset.attributes["label"].read(value);
  if (value != label) return DatasetTestFailed(name, "has wrong label attribute");
  return ::testing::AssertionSuccess();
}


template<class T=void>
::testing::AssertionResult DatasetHasUnit(
  hdf5::node::Group const &group,
  std::string const &name,
  std::string_view unit) {
  const auto dataset = group.get_dataset(name);
  if (!dataset.attributes.exists("units")) return DatasetTestFailed(name, "has no unit attribute");
  std::string ds_unit;
  dataset.attributes["units"].read(ds_unit);
  if (ds_unit != unit) return DatasetTestFailed(name, "has wrong unit attribute");
  return ::testing::AssertionSuccess();
}


template<class T=void>
::testing::AssertionResult DatasetHasShape(
  hdf5::node::Group const &group,
  std::string const &name,
  std::vector<hsize_t> const &shape,
  std::vector<hsize_t> const &maximum_shape) {
  const auto dataset = group.get_dataset(name);
  // If the shape is empty, the dataset is a scalar:
  auto dataspace_type = dataset.dataspace().type();
  if (dataspace_type == hdf5::dataspace::Type::Scalar) {
    if (shape.empty()) return ::testing::AssertionSuccess();
    std::stringstream message;
    message << "has wrong dataspace type, " << dataspace_type << " != " << hdf5::dataspace::Type::Scalar;
    return DatasetTestFailed(name, message.str());
  }
  // otherwise it is a simple dataspace: (no other types implemented)
  auto size = std::accumulate(shape.begin(), shape.end(), 1, std::multiplies<hsize_t>());
  if (dataset.dataspace().size() != size) {
    std::stringstream message;
    message << "has wrong size, " << dataset.dataspace().size() << " != " << size;
    return DatasetTestFailed(name, message.str());
  }
  auto dataspace = hdf5::dataspace::Simple(dataset.dataspace());
  if (dataspace.rank() != shape.size()) return DatasetTestFailed(name, "has wrong rank");
  auto current = dataspace.current_dimensions();
  std::stringstream message;
  for (size_t i = 0; i < shape.size(); ++i) {
    if (current[i] != shape[i]) {
      message << "has wrong extent along dimension ";
      message << i << ", " << current[i] << " != " << shape[i];
      return DatasetTestFailed(name, message.str());
    }
  }
  auto maximum = dataspace.maximum_dimensions();
  for (size_t i = 0; i < shape.size(); ++i) {
    if (maximum[i] != maximum_shape[i]) {
      message << "has wrong maximum extent along dimension ";
       message << i << ", " << maximum[i] << " != " << maximum_shape[i];
      return DatasetTestFailed(name, message.str());
    }
  }
  return ::testing::AssertionSuccess();
}

template<class T=void>
::testing::AssertionResult DatasetHasDims(
    hdf5::node::Group const & group,
    std::string const & name,
    std::vector<std::string> const & expected){
  const auto dataset = group.get_dataset(name);
  if (!dataset.attributes.exists("axes"))
    return DatasetTestFailed(name, "has no 'axes' attribute");
  auto dims = dataset.attributes["axes"];
  if (dims.dataspace().size() != static_cast<hssize_t>(expected.size()))
    return DatasetTestFailed(name, "has wrong number of 'axes' dims");
  std::vector<std::string> ds_dims(dims.dataspace().size());
  dims.read(ds_dims);
  if (ds_dims != expected)
    return DatasetTestFailed(name, "has wrong 'axes' dims attribute");
  std::cout << "Dataset " << name << " has dims: [";
        for (auto const & dim : ds_dims) std::cout << " " << dim;
        std::cout << " ]" << std::endl;
  return ::testing::AssertionSuccess();
}
template<class T>
::testing::AssertionResult DatasetHasData(
  hdf5::node::Group const &group,
  std::string const &name,
  std::vector<T> const &data) {
  auto dataset = group.get_dataset(name);
  std::vector<T> read_data(data.size());
  dataset.read(read_data);
  bool passed{true};
  std::string failure_message;
  if constexpr (std::is_integral_v<T>) {
    passed = read_data == data;
  } else {
    for (size_t i = 0; i < data.size(); ++i) {
      passed &= std::abs(data[i] - data[i]) / std::abs(read_data[i] + read_data[i]) <= 1e-6;
    }
  }
  if (!passed) {
    std::stringstream buffer;
    buffer << "has wrong data: [";
    for (size_t i = 0; i < data.size(); ++i) {
      buffer << " " << data[i] << " != " << read_data[i] << ",";
    }
    buffer.seekp(-1, std::ios_base::end);
    buffer << " ]";
    failure_message = buffer.str();
  }
  return passed ? ::testing::AssertionSuccess() : DatasetTestFailed(name, failure_message);
}


class da00_WriterStandIn : public WriterModule::da00::da00_Writer {
public:
  using da00_Writer::ChunkSize;
  using da00_Writer::Timestamp;
  using da00_Writer::CueTimestampZero;
  using da00_Writer::CueIndex;
  using da00_Writer::isFirstMessage;
  using da00_Writer::ConstantsField;
  using da00_Writer::VariablesField;
  using da00_Writer::VariableMap;
  using da00_Writer::VariableNames;
  using da00_Writer::VariablePtrs;
  using da00_Writer::ConstantNames;
  using da00_Writer::ConstantPtrs;
};


class da00_VariableConfigTestFixture : public ::testing::Test {
public:
  using da00_Writer = WriterModule::da00::da00_Writer;
};
using json = nlohmann::json;

json createVariableJson(){
  return json::parse(R"""({
    "name": "signal",
    "unit": "counts",
    "label": "Integrated detector counts",
    "data_type": "uint64",
    "dims": ["x", "y"],
    "shape": [10, 10]
  })""");
}

TEST_F(da00_VariableConfigTestFixture, VariableWithoutNameThrows){
    json VariableJson = createVariableJson();
    VariableJson.erase("name");
    WriterModule::da00::VariableConfig variable;
    auto json_string = VariableJson.dump();
    EXPECT_THROW(variable = json_string;, nlohmann::detail::type_error);
}

TEST_F(da00_VariableConfigTestFixture, VariableWithOnlyNameWorks){
    json ConstantJson = createVariableJson();
    ConstantJson.erase("unit");
    ConstantJson.erase("label");
    ConstantJson.erase("data_type");
    ConstantJson.erase("dims");
    ConstantJson.erase("shape");
    WriterModule::da00::VariableConfig constant;
    auto json_string = ConstantJson.dump();
    EXPECT_NO_THROW(constant = json_string;);
}

TEST_F(da00_VariableConfigTestFixture, VariableConfigCreation){
    json VariableJson = createVariableJson();
    WriterModule::da00::VariableConfig variable;
    auto json_string = VariableJson.dump();
    EXPECT_NO_THROW(variable = json_string;);
    EXPECT_EQ(variable.has_unit(), true);
    EXPECT_EQ(variable.has_label(), true);
    EXPECT_EQ(variable.has_type(), true);
    EXPECT_EQ(variable.has_dims(), true);
    EXPECT_EQ(variable.has_shape(), true);
    EXPECT_EQ(variable.has_data(), false);
    EXPECT_EQ(variable.name(), "signal");
    EXPECT_EQ(variable.unit(), "counts");
    EXPECT_EQ(variable.label(), "Integrated detector counts");
    EXPECT_EQ(variable.type(), WriterModule::da00::da00_type::uint64);
    EXPECT_EQ(variable.dims(), std::vector<std::string>({"x", "y"}));
    EXPECT_EQ(variable.shape(), std::vector<hsize_t>({10, 10}));
}

json createConstantJson(){
    return json::parse(R"""({
    "name": "x",
    "unit": "cm",
    "label": "Position along x-axis",
    "data_type": "float32",
    "dims": ["x"],
    "shape": [5],
    "data": [0, 0.1, 0.2, 0.3, 0.4]
})""");
}

TEST_F(da00_VariableConfigTestFixture, ConstantWithoutNameThrows){
    json VariableJson = createConstantJson();
    VariableJson.erase("name");
    WriterModule::da00::VariableConfig variable;
    auto json_string = VariableJson.dump();
    EXPECT_THROW(variable = json_string;, nlohmann::detail::type_error);
}


TEST_F(da00_VariableConfigTestFixture, ConstantWithOnlyNameWorks){
    json ConstantJson = createConstantJson();
    ConstantJson.erase("unit");
    ConstantJson.erase("label");
    ConstantJson.erase("data_type");
    ConstantJson.erase("dims");
    ConstantJson.erase("shape");
    ConstantJson.erase("data");
    WriterModule::da00::VariableConfig constant;
    auto json_string = ConstantJson.dump();
    EXPECT_NO_THROW(constant = json_string;);
}

TEST_F(da00_VariableConfigTestFixture, ConstantConfigCreation){
    json VariableJson = createConstantJson();
    WriterModule::da00::VariableConfig constant;
    auto json_string = VariableJson.dump();
    EXPECT_NO_THROW(constant = json_string;);
    EXPECT_EQ(constant.has_unit(), true);
    EXPECT_EQ(constant.has_label(), true);
    EXPECT_EQ(constant.has_type(), true);
    EXPECT_EQ(constant.has_dims(), true);
    EXPECT_EQ(constant.has_shape(), true);
    EXPECT_EQ(constant.has_data(), true);
    EXPECT_EQ(constant.name(), "x");
    EXPECT_EQ(constant.unit(), "cm");
    EXPECT_EQ(constant.label(), "Position along x-axis");
    EXPECT_EQ(constant.type(), WriterModule::da00::da00_type::float32);
    EXPECT_EQ(constant.dims(), std::vector<std::string>({"x"}));
    EXPECT_EQ(constant.shape(), std::vector<hsize_t>({5}));
    EXPECT_EQ(constant.data<float>(), std::vector<float>({0, 0.1, 0.2, 0.3, 0.4}));
}

TEST_F(da00_VariableConfigTestFixture, ConstantConfigCreationWithoutData){
    json VariableJson = createConstantJson();
    VariableJson.erase("data");
    WriterModule::da00::VariableConfig constant;
    auto json_string = VariableJson.dump();
    EXPECT_NO_THROW(constant = json_string;);
    EXPECT_EQ(constant.has_unit(), true);
    EXPECT_EQ(constant.has_label(), true);
    EXPECT_EQ(constant.has_type(), true);
    EXPECT_EQ(constant.has_dims(), true);
    EXPECT_EQ(constant.has_shape(), true);
    EXPECT_EQ(constant.has_data(), false);
    EXPECT_EQ(constant.name(), "x");
    EXPECT_EQ(constant.unit(), "cm");
    EXPECT_EQ(constant.label(), "Position along x-axis");
    EXPECT_EQ(constant.type(), WriterModule::da00::da00_type::float32);
    EXPECT_EQ(constant.dims(), std::vector<std::string>({"x"}));
    EXPECT_EQ(constant.shape(), std::vector<hsize_t>({5}));
}

json createMinMaxSizeConstantJson(){
    return json::parse(R"""({
    "name": "x",
    "unit": "cm",
    "label": "Position along x-axis",
    "data_type": "float32",
    "dims": ["x"],
    "shape": [5],
    "data": {"first": 0.0, "last": 0.4, "size": 5}
})""");
}

TEST_F(da00_VariableConfigTestFixture, ConstantConfigMinMaxSizeCreation){
    json VariableJson = createMinMaxSizeConstantJson();
    WriterModule::da00::VariableConfig constant;
    auto json_string = VariableJson.dump();
    EXPECT_NO_THROW(constant = json_string;);
    EXPECT_EQ(constant.has_unit(), true);
    EXPECT_EQ(constant.has_label(), true);
    EXPECT_EQ(constant.has_type(), true);
    EXPECT_EQ(constant.has_dims(), true);
    EXPECT_EQ(constant.has_shape(), true);
    EXPECT_EQ(constant.has_data(), true);
    EXPECT_EQ(constant.name(), "x");
    EXPECT_EQ(constant.unit(), "cm");
    EXPECT_EQ(constant.label(), "Position along x-axis");
    EXPECT_EQ(constant.type(), WriterModule::da00::da00_type::float32);
    EXPECT_EQ(constant.dims(), std::vector<std::string>({"x"}));
    EXPECT_EQ(constant.shape(), std::vector<hsize_t>({5}));
    EXPECT_EQ(constant.data<float>(), std::vector<float>({0, 0.1, 0.2, 0.3, 0.4}));
}


class da00_EdgeConfigTestFixture :public ::testing::TestWithParam<json> {
  public:
    using da00_Writer = WriterModule::da00::da00_Writer;

};

TEST_P(da00_EdgeConfigTestFixture, EdgeConfigErrors){
    auto input = GetParam();
    WriterModule::da00::VariableConfig constant;
    constant = input.dump();
    try {
      auto data = constant.data<double>();
      std::cout << "(Unexpected) Success! [";
      for (const auto & x: data) std::cout << " " << x;;
      std::cout << " ]" << std::endl;
      FAIL();
    } catch (std::runtime_error const &e) {
      ASSERT_EQ(e.what(), std::string("Invalid EdgeConfig JSON input"));
    }
}

INSTANTIATE_TEST_SUITE_P(
        da00_EdgeConfigErrorTest, da00_EdgeConfigTestFixture,
            ::testing::Values(
    json::parse(R"""({"name": "x", "data": "x"})"""),
    json::parse(R"""({"name": "x", "data": {"first": 0.0, "last": 0.4}})"""),
    json::parse(R"""({"name": "x", "data": {"first": 0.0, "size": 5}})"""),
    json::parse(R"""({"name": "x", "data": {"last": 0.4, "size": 5}})"""),
    json::parse(R"""({"name": "x", "data": 1})"""),
    json::parse(R"""({"name": "x", "data": {"values": [0, 0.1, 0.2, 0.3, 0.4]}})""")
));

class da00_DataTypeTestFixture :public ::testing::TestWithParam<std::pair<json, WriterModule::da00::da00_type>> {
  public:
  using da00_Writer = WriterModule::da00::da00_Writer;
};

TEST_P(da00_DataTypeTestFixture, DataTypeTest){
    auto input = GetParam();
    WriterModule::da00::VariableConfig constant;
    constant = input.first.dump();
    EXPECT_EQ(constant.type(), input.second);
}

auto make_datatypetest_input(std::string const &type) {
    auto j = json::parse(R"""({"name": "x", "data_type": ")""" + type + R"""("})""");
    auto t = WriterModule::da00::string_to_da00_type(type);
    return std::make_pair(j, t);
}

INSTANTIATE_TEST_SUITE_P(
    da00_DataTypeTest, da00_DataTypeTestFixture,
    ::testing::Values(
        make_datatypetest_input("int8"),
        make_datatypetest_input("uint8"),
        make_datatypetest_input("int16"),
        make_datatypetest_input("uint16"),
        make_datatypetest_input("int32"),
        make_datatypetest_input("uint32"),
        make_datatypetest_input("int64"),
        make_datatypetest_input("uint64"),
        make_datatypetest_input("float32"),
        make_datatypetest_input("float64"),
        make_datatypetest_input("c_string")
        ));

template<class T>
auto insert_variable(flatbuffers::FlatBufferBuilder & builder,
                     std::string const & name, std::string const & unit,
                     std::string const & label, std::vector<T> data,
                     const std::vector<int64_t> & shape,
                     const std::vector<std::string> & dims
                     ) {
    using namespace WriterModule::da00;
    auto name_o = builder.CreateString(name);
    auto unit_o = builder.CreateString(unit);
    auto label_o = builder.CreateString(label);
    auto type = get_dtype<T>(T());
    auto dims_o = builder.CreateVectorOfStrings(dims);
    auto shape_o = builder.CreateVector(shape);
    auto ptr = reinterpret_cast<uint8_t*>(data.data());
    auto count = data.size() * sizeof(T) / sizeof(uint8_t);
    const std::vector<uint8_t> data_span(ptr, ptr + count);
    auto data_o = builder.CreateVector<uint8_t>(data_span);
    return CreateVariable(builder, name_o, unit_o, label_o, type, dims_o, shape_o, data_o);
}

auto make_da00_message(){
  using namespace WriterModule::da00;
  flatbuffers::FlatBufferBuilder builder;
  auto signal_variable = insert_variable<uint64_t>(
      builder, "signal", "counts", "Integrated counts on strip detector",
      {1, 2, 3, 4, 5, 6, 7, 8, 9}, {3, 3}, {"x", "y"});
  auto x_variable = insert_variable<float>(
      builder, "x", "cm", "Binned position along x-axis",
      {10., 20.1, 30.2, 40.3}, {4}, {"x"});
  auto y_variable = insert_variable<int8_t>(
      builder, "y", "fm", "Position along y-axis", {9, 6, 3}, {3}, {"y"});

  auto source_name = builder.CreateString("test_source_name");
  auto variables = builder.CreateVector(std::vector<flatbuffers::Offset<Variable>>{signal_variable, x_variable, y_variable});


  int32_t id = 512;
  uint64_t timestamp = 19820909;
  auto data_array = Createda00_DataArray(builder, source_name, id, timestamp,  variables);
  builder.Finish(data_array, da00_DataArrayIdentifier());
//  Finishda00_DataArrayBuffer(builder, data_array);
  // take ownership of the buffer
  //return FileWriter::FlatbufferMessage(builder.GetBufferPointer(), builder.GetSize());
  return builder.Release(); // returns a flatbuffers::DetachedBuffer
}

json make_da00_configuration_complete(){
  return json::parse(R"""({
    "topic": "test.topic.name",
    "source": "test_producer_name",
    "variables": ["signal"],
    "constants": ["x", "y"],
    "attributes" : [
        {"name": "test_attribute", "value": "test_value"}
    ],
    "datasets": [
        {
          "name": "signal",
          "unit": "counts",
          "label": "Integrated counts on strip detector",
          "data_type": "uint64",
          "dims": ["x", "y"],
          "shape": [3, 3]
        },
        {
          "name": "x",
          "unit": "cm",
          "label": "Binned position along x-axis",
          "data_type": "float32",
          "dims": ["x"],
          "shape": [4],
          "data": [10.0, 20.1, 30.2, 40.3]
        },
        {
          "name": "y",
          "unit": "fm",
          "label": "Position along y-axis",
          "data_type": "int8",
          "dims": ["y"],
          "shape": [3],
          "data": [9, 6, 3]
        }
      ]
  })""");
}

json make_da00_configuration_partial(){
  return json::parse(R"""({
    "topic": "test.topic.name",
    "source": "test_producer_name",
    "variables": ["signal"],
    "constants": ["x", "y"],
    "datasets": [
        {
          "name": "signal",
          "dims": ["x", "y"]
        },
        {
          "name": "x",
          "dims": ["x"]
        },
        {
          "name": "y",
          "dims": ["y"]
        }
      ]
  })""");
}


json make_da00_configuration_preconfigure_minimal(){
  return json::parse(R"""({
    "topic": "test.topic.name",
    "source": "test_producer_name",
    "variables": ["signal"],
    "constants": ["x", "y"],
    "datasets": [
        {
          "name": "signal",
          "data_type": "uint64",
          "shape": [3, 3]
        },
        {
          "name": "x",
          "shape": [4],
        },
        {
          "name": "y",
          "shape": [3],
        }
      ]
  })""");
}

json make_da00_configuration_abbreviated(){
  return json::parse(R"""({
    "topic": "test.topic.name",
    "source": "test_producer_name",
    "variables": ["signal"],
    "constants": ["x", "y"],
    "datasets": [
        {
          "name": "signal",
          "unit": "counts",
          "label": "Integrated counts on strip detector",
          "data_type": "uint64",
          "dims": ["x", "y"],
          "shape": [3, 3]
        },
        {
          "name": "x",
          "unit": "cm",
          "label": "Binned position along x-axis",
          "data_type": "float32",
          "dims": ["x"],
          "shape": [4],
          "data": {"first": 10, "last": 40.3, "size": 4}
        },
        {
          "name": "y",
          "unit": "fm",
          "label": "Position along y-axis",
          "data_type": "int8",
          "dims": ["y"],
          "shape": [3],
          "data": {"first": 9, "last": 3, "size": 3}
        }
      ]
  })""");
}

json make_da00_configuration_minimal(){
  return json::parse(R"""({
    "topic": "test.topic.name",
    "source": "test_producer_name",
    "variables": ["signal"],
    "constants": ["x", "y"]
  })""");
}

json make_da00_configuration_automatic(){
  return json::parse(R"""({
    "topic": "test.topic.name",
    "source": "test_producer_name"
  })""");
}


class da00_WriterTestFixture : public ::testing::Test {
  public:
    using da00_Writer = WriterModule::da00::da00_Writer;
    static void SetUpTestSuite() {
      setExtractorModule<AccessMessageMetadata::da00_Extractor>("da00");
      WriterModule::Registry::Registrar<da00_Writer>("da00", "test_name");
    }
    void SetUp() override {
      const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
      _log_group_name = test_info->name();
      _file = HDFFileTestHelper::createInMemoryTestFile(_test_file_name, true);
      _root = _file->hdfGroup();
      _group = _root.create_group(_log_group_name);
      _buffer = make_da00_message();
      setExtractorModule<AccessMessageMetadata::da00_Extractor>("da00");
    }
    std::unique_ptr<HDFFileTestHelper::DebugHDFFile> _file;
    std::string _test_file_name{"test_da00_file.hdf"};
    std::string _log_group_name{"some_parent_name"};
    hdf5::node::Group _root;
    hdf5::node::Group _group;
    flatbuffers::DetachedBuffer _buffer;
};

TEST_F(da00_WriterTestFixture, da00_WriterCanNotOpenUninitializedGroup) {
  auto writer = da00_WriterStandIn();
  EXPECT_FALSE(writer.reopen(_group) == WriterModule::InitResult::OK);
}

TEST_F(da00_WriterTestFixture, da00_WriterCanOpenUnconfiguredInitializedGroup) {
  auto writer = da00_WriterStandIn();
  EXPECT_TRUE(writer.init_hdf(_group) == WriterModule::InitResult::OK);
  EXPECT_TRUE(writer.reopen(_group) == WriterModule::InitResult::OK);
}

TEST_F(da00_WriterTestFixture, da00_WriterCanOpenConfiguredInitializedGroup) {
  auto writer = da00_WriterStandIn();
  writer.parse_config(make_da00_configuration_complete().dump());
  EXPECT_TRUE(writer.init_hdf(_group) == WriterModule::InitResult::OK);
  EXPECT_TRUE(writer.reopen(_group) == WriterModule::InitResult::OK);
}

TEST_F(da00_WriterTestFixture, da00_WriterInit) {
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(make_da00_configuration_complete().dump());
    writer.init_hdf(_group);
  }
  for (const auto & name: {"signal", "axes", "test_attribute"}) {
    EXPECT_TRUE(NodeHasAttribue(_group, name));
  }
  for (const auto &name : {"signal", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
  const std::vector<hsize_t> signal_shape{0, 3, 3}, x_shape{4}, y_shape{3};
  const std::vector<hsize_t> max_signal_shape{H5S_UNLIMITED, H5S_UNLIMITED, H5S_UNLIMITED};
  EXPECT_TRUE(DatasetHasUnit(_group, "signal", "counts"));
  EXPECT_TRUE(DatasetHasLabel(_group, "signal", "Integrated counts on strip detector"));
  EXPECT_TRUE(DatasetHasShape(_group, "signal", signal_shape, max_signal_shape));

  const auto x_data = std::vector<float>{{10.0, 20.1, 30.2, 40.3}};
  const auto y_data = std::vector<std::int8_t>{{9, 6, 3}};
  EXPECT_TRUE(DatasetHasUnit(_group, "x", "cm"));
  EXPECT_TRUE(DatasetHasLabel(_group, "x", "Binned position along x-axis"));
  EXPECT_TRUE(DatasetHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(DatasetHasData(_group, "x", x_data));

  EXPECT_TRUE(DatasetHasUnit(_group, "y", "fm"));
  EXPECT_TRUE(DatasetHasLabel(_group, "y", "Position along y-axis"));
  EXPECT_TRUE(DatasetHasShape(_group, "y", y_shape, y_shape));
  EXPECT_TRUE(DatasetHasData(_group, "y", y_data));

  const std::vector<hsize_t> time_shape{0}, max_time_shape{H5S_UNLIMITED};
  EXPECT_TRUE(DatasetHasShape(_group, "time", time_shape, max_time_shape));

}

TEST_F(da00_WriterTestFixture, da00_WriterConfigInitMakeGroups) {
  auto message = FileWriter::FlatbufferMessage(_buffer.data(), _buffer.size());
  auto config = make_da00_configuration_complete();
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(config.dump());
    writer.init_hdf(_group);
  }
  for (const auto &name : {"signal", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
}
TEST_F(da00_WriterTestFixture, da00_WriterConfigReopenKeepsGroups) {
  auto message = FileWriter::FlatbufferMessage(_buffer.data(), _buffer.size());
  auto config = make_da00_configuration_complete();
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(config.dump());
    writer.init_hdf(_group);
  }
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(config.dump());
    writer.reopen(_group);
  }
  for (const auto &name : {"signal", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
}

TEST_F(da00_WriterTestFixture, da00_WriterConfigReopenGroupsHaveCorrectShape) {
  {
    auto config = make_da00_configuration_complete();
    auto writer = da00_WriterStandIn();
    writer.parse_config(config.dump());
    writer.init_hdf(_group);
    writer.reopen(_group);
  }
  for (const auto &name : {"signal", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
  const std::vector<hsize_t> signal_shape{0, 3, 3}, x_shape{4}, y_shape{3};
  const std::vector<hsize_t> max_signal_shape{H5S_UNLIMITED, H5S_UNLIMITED, H5S_UNLIMITED};
  const std::vector<std::string> signal_dims={"x", "y"};
  EXPECT_TRUE(DatasetHasUnit(_group, "signal", "counts"));
  EXPECT_TRUE(DatasetHasLabel(_group, "signal", "Integrated counts on strip detector"));
  EXPECT_TRUE(DatasetHasShape(_group, "signal", signal_shape, max_signal_shape));
  EXPECT_TRUE(DatasetHasDims(_group, "signal", signal_dims));

  const auto x_data = std::vector<float>{{10.0, 20.1, 30.2, 40.3}};
  const auto y_data = std::vector<std::int8_t>{{9, 6, 3}};
  EXPECT_TRUE(DatasetHasUnit(_group, "x", "cm"));
  EXPECT_TRUE(DatasetHasLabel(_group, "x", "Binned position along x-axis"));
  EXPECT_TRUE(DatasetHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(DatasetHasData(_group, "x", x_data));
  EXPECT_TRUE(DatasetHasUnit(_group, "y", "fm"));
  EXPECT_TRUE(DatasetHasLabel(_group, "y", "Position along y-axis"));
  EXPECT_TRUE(DatasetHasShape(_group, "y", y_shape, y_shape));
  EXPECT_TRUE(DatasetHasData(_group, "y", y_data));
  const std::vector<hsize_t> time_shape{0}, max_time_shape{H5S_UNLIMITED};
  EXPECT_TRUE(DatasetHasShape(_group, "time", time_shape, max_time_shape));
}

TEST_F(da00_WriterTestFixture, da00_WriterInitPartialConfig) {
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(make_da00_configuration_partial().dump());
    writer.init_hdf(_group);
  }
  for (const auto & dataset : {"signal", "x", "y"})
    EXPECT_FALSE(DatasetIsValid(_group, dataset));
  for (const auto &name : {"time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
}

TEST_F(da00_WriterTestFixture, da00_WriterInitAbbreviatedConfig) {
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(make_da00_configuration_abbreviated().dump());
    writer.init_hdf(_group);
  }
  for (const auto &name : {"signal", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
  const std::vector<hsize_t> x_shape{4}, y_shape{3};
  const auto x_data = std::vector<float>{{10.0, 20.1, 30.2, 40.3}};
  const auto y_data = std::vector<std::int8_t>{{9, 6, 3}};
  EXPECT_TRUE(DatasetHasUnit(_group, "x", "cm"));
  EXPECT_TRUE(DatasetHasLabel(_group, "x", "Binned position along x-axis"));
  EXPECT_TRUE(DatasetHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(DatasetHasData(_group, "x", x_data));
  EXPECT_TRUE(DatasetHasUnit(_group, "y", "fm"));
  EXPECT_TRUE(DatasetHasLabel(_group, "y", "Position along y-axis"));
  EXPECT_TRUE(DatasetHasShape(_group, "y", y_shape, y_shape));
  EXPECT_TRUE(DatasetHasData(_group, "y", y_data));
}

TEST_F(da00_WriterTestFixture, da00_WriterInitMinimalConfig) {
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(make_da00_configuration_minimal().dump());
    writer.init_hdf(_group);
    EXPECT_TRUE(std::find(writer.VariableNames.begin(), writer.VariableNames.end(), "signal") != writer.VariableNames.end());
    EXPECT_TRUE(std::find(writer.ConstantNames.begin(), writer.ConstantNames.end(), "x") != writer.ConstantNames.end());
    EXPECT_TRUE(std::find(writer.ConstantNames.begin(), writer.ConstantNames.end(), "y") != writer.ConstantNames.end());
  }
  for (const auto &name : {"time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
  // Even though the variables and constants are known from the configuration,
  // their datasets are not created until the first message is received.
  for (const auto &name : {"signal", "x", "y"}) {
    EXPECT_FALSE(DatasetIsValid(_group, name));
  }
}

TEST_F(da00_WriterTestFixture, da00_WriterInitAutomaticConfig) {
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(make_da00_configuration_automatic().dump());
    writer.init_hdf(_group);
    EXPECT_TRUE(writer.VariableNames.empty());
    EXPECT_TRUE(writer.ConstantNames.empty());
  }
  for (const auto &name : {"time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
  // The configuration did not name any variables or constants, so they are not created
  // until the first message is received.
  for (const auto &name : {"signal", "x", "y"}) {
    EXPECT_FALSE(DatasetIsValid(_group, name));
  }
}

TEST_F(da00_WriterTestFixture, da00_WriterWriteExtendsShape){
  using hdf5::dataspace::Simple;
  auto message = FileWriter::FlatbufferMessage(_buffer.data(), _buffer.size());
  auto config = make_da00_configuration_complete();
  constexpr int write_count{13};
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(config.dump());
    writer.init_hdf(_group);
    writer.reopen(_group);
    for (int i=0; i<write_count; ++i) EXPECT_NO_THROW(writer.write(message));
    EXPECT_EQ(write_count, writer.Timestamp.dataspace().size());
  }
  for (const auto &name : {"signal", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
  // variables are extended (but constants are not)
  const std::vector<hsize_t> signal_shape{write_count, 3, 3}, x_shape{4}, y_shape{3};
  const std::vector<hsize_t> max_signal_shape{H5S_UNLIMITED, H5S_UNLIMITED, H5S_UNLIMITED};
  EXPECT_TRUE(DatasetHasShape(_group, "signal", signal_shape, max_signal_shape));
  EXPECT_TRUE(DatasetHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(DatasetHasShape(_group, "y", y_shape, y_shape));
}

TEST_F(da00_WriterTestFixture, da00_WriterWriteCopiesData){
  using hdf5::dataspace::Simple;
  auto message = FileWriter::FlatbufferMessage(_buffer.data(), _buffer.size());
  auto config = make_da00_configuration_complete();
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(config.dump());
    writer.init_hdf(_group);
    writer.reopen(_group);
    constexpr int write_count{10};
    for (int i=0; i<write_count; ++i) EXPECT_NO_THROW(writer.write(message));
    EXPECT_EQ(write_count, writer.Timestamp.dataspace().size());
  }
  const auto shape = Simple(_group.get_dataset("signal").dataspace());
  std::vector<uint64_t> data(shape.size());
  const std::vector<uint64_t> sent{1, 2, 3, 4, 5, 6, 7, 8, 9};;
  _group.get_dataset("signal").read(data);
  for (size_t i=0; i<data.size(); i+=sent.size()) {
    ASSERT_THAT(std::vector(data.data() + i, data.data() + i + sent.size()),
      ::testing::ElementsAreArray(sent));
  }
}

TEST_F(da00_WriterTestFixture, da00_WriterWritePartialDefinesPropertiesAndShape){
  using hdf5::dataspace::Simple;
  auto message = FileWriter::FlatbufferMessage(_buffer.data(), _buffer.size());
  constexpr int write_count{101};
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(make_da00_configuration_partial().dump());
    writer.init_hdf(_group);
    writer.reopen(_group);
    for (int i=0; i<write_count; ++i) EXPECT_NO_THROW(writer.write(message));
    EXPECT_EQ(write_count, writer.Timestamp.dataspace().size());
  }
  for (const auto &name : {"signal", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
  // variables are extended, and the constant shapes are defied by the first message
  const std::vector<hsize_t> signal_shape{write_count, 3, 3}, x_shape{4}, y_shape{3};
  const std::vector<hsize_t> max_signal_shape{H5S_UNLIMITED, H5S_UNLIMITED, H5S_UNLIMITED};
  const auto x_data = std::vector<float>{{10.0, 20.1, 30.2, 40.3}};
  const auto y_data = std::vector<std::int8_t>{{9, 6, 3}};

  EXPECT_TRUE(DatasetHasUnit(_group, "signal", "counts"));
  EXPECT_TRUE(DatasetHasShape(_group, "signal", signal_shape, max_signal_shape));
  EXPECT_TRUE(DatasetHasUnit(_group, "x", "cm"));
  EXPECT_TRUE(DatasetHasLabel(_group, "x", "Binned position along x-axis"));
  EXPECT_TRUE(DatasetHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(DatasetHasData(_group, "x", x_data));
  EXPECT_TRUE(DatasetHasUnit(_group, "y", "fm"));
  EXPECT_TRUE(DatasetHasLabel(_group, "y", "Position along y-axis"));
  EXPECT_TRUE(DatasetHasShape(_group, "y", y_shape, y_shape));
  EXPECT_TRUE(DatasetHasData(_group, "y", y_data));
}

TEST_F(da00_WriterTestFixture, da00_WriterWriteMinimalExtendsShape){
  using hdf5::dataspace::Simple;
  auto message = FileWriter::FlatbufferMessage(_buffer.data(), _buffer.size());
  constexpr int write_count{23};
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(make_da00_configuration_minimal().dump());
    writer.init_hdf(_group);
    writer.reopen(_group);
    for (int i=0; i<write_count; ++i) EXPECT_NO_THROW(writer.write(message));
    EXPECT_EQ(write_count, writer.Timestamp.dataspace().size());
  }
  for (const auto &name : {"signal", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
  // variables are extended, and the constant shapes are defied by the first message
  const std::vector<hsize_t> signal_shape{write_count, 3, 3}, x_shape{4}, y_shape{3};
  const std::vector<hsize_t> max_signal_shape{H5S_UNLIMITED, H5S_UNLIMITED, H5S_UNLIMITED};
  EXPECT_TRUE(DatasetHasShape(_group, "signal", signal_shape, max_signal_shape));
  EXPECT_TRUE(DatasetHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(DatasetHasShape(_group, "y", y_shape, y_shape));
}

TEST_F(da00_WriterTestFixture, da00_WriterWriteAutomaticExtendsShape){
  using hdf5::dataspace::Simple;
  auto message = FileWriter::FlatbufferMessage(_buffer.data(), _buffer.size());
  constexpr int write_count{13};
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(make_da00_configuration_automatic().dump());
    writer.init_hdf(_group);
    writer.reopen(_group);
    for (int i=0; i<write_count; ++i) EXPECT_NO_THROW(writer.write(message));
    EXPECT_EQ(write_count, writer.Timestamp.dataspace().size());
  }
  for (const auto &name : {"signal", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(DatasetIsValid(_group, name));
  }
  // All message-contained Variables are treated as variable datasets:
  const std::vector<hsize_t> signal_shape{write_count, 3, 3}, x_shape{write_count, 4}, y_shape{write_count, 3};
  const std::vector<hsize_t> max_signal_shape{H5S_UNLIMITED, H5S_UNLIMITED, H5S_UNLIMITED};
  const std::vector<hsize_t> max_xy_shape{H5S_UNLIMITED, H5S_UNLIMITED};
  EXPECT_TRUE(DatasetHasShape(_group, "signal", signal_shape, max_signal_shape));
  EXPECT_TRUE(DatasetHasShape(_group, "x", x_shape, max_xy_shape));
  EXPECT_TRUE(DatasetHasShape(_group, "y", y_shape, max_xy_shape));
}

//
// enum class FileCreationLocation {agnostic, memory, disk};
// hdf5::file::File createFile(const std::string& name, FileCreationLocation location) {
//     using FCL = FileCreationLocation;
//     hdf5::property::FileAccessList FAL;
//     if (location == FCL::agnostic) location = FCL::memory;
//     if (location == FCL::memory) {
//       FAL.driver(hdf5::file::MemoryDriver());
//     }
//     return hdf5::file::create(name, hdf5::file::AccessFlags::Truncate,
//                               hdf5::property::FileCreationList(), FAL);
// }
//
// TEST_F(da00_VariableConfigTestFixture, VariableCreateHDFStructure){
//
// }
