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


static ::testing::AssertionResult NodeTestFailed(std::string_view name, std::string_view message) {
  return ::testing::AssertionFailure() << "Dataset " << name << " " << message;
}

static ::testing::AssertionResult NodeHasAttribue(
  hdf5::node::Group const &group, std::string const &name) {
  if (!group.attributes.exists(name))
    return NodeTestFailed(group.link().path().operator std::string(),
                          "has no attribute " + name);
  return ::testing::AssertionSuccess();
}

static ::testing::AssertionResult NodeIsValid(
  hdf5::node::Group const &group,
  const std::string & name) {
  if (!group.has_dataset(name)) return NodeTestFailed(name, "not found");
  const auto dataset = group.get_dataset(name);
  if (!dataset.dataspace().is_valid()) return NodeTestFailed(name, "is not valid");
  return ::testing::AssertionSuccess();
}

static std::string get_fixed_string(const hdf5::attribute::Attribute & attr) {
  std::string value;
  attr.read(value);
  auto pos = value.find('\0');
  if (pos != std::string::npos) value.resize(pos);
  return value;
}
static ::testing::AssertionResult NodeHasLabel(
  hdf5::node::Group const &group,
  const std::string & name,
  std::string_view label) {
  auto dataset = group.get_dataset(name);
  if (!dataset.attributes.exists("long_name")) return NodeTestFailed(name, "has no long_name attribute");
  auto value = get_fixed_string(dataset.attributes["long_name"]);
  if (value != label) return NodeTestFailed(name, fmt::format( "has wrong long_name attribute; {} != {}", value, label));
  return ::testing::AssertionSuccess();
}

static ::testing::AssertionResult NodeHasUnit(
  hdf5::node::Group const &group,
  std::string const &name,
  std::string_view unit) {
  const auto dataset = group.get_dataset(name);
  if (!dataset.attributes.exists("units")) return NodeTestFailed(name, "has no unit attribute");
  auto ds_unit = get_fixed_string(dataset.attributes["units"]);
  if (ds_unit != unit) return NodeTestFailed(name, fmt::format("has wrong unit attribute; {} != {}", ds_unit, unit));
  return ::testing::AssertionSuccess();
}

static ::testing::AssertionResult NodeHasShape(
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
    return NodeTestFailed(name, message.str());
  }
  // otherwise it is a simple dataspace: (no other types implemented)
  auto size = std::accumulate(shape.begin(), shape.end(), 1, std::multiplies<hsize_t>());
  if (dataset.dataspace().size() != size) {
    std::stringstream message;
    message << "has wrong size, " << dataset.dataspace().size() << " != " << size;
    return NodeTestFailed(name, message.str());
  }
  auto dataspace = hdf5::dataspace::Simple(dataset.dataspace());
  if (dataspace.rank() != shape.size()) return NodeTestFailed(name, "has wrong rank");
  auto current = dataspace.current_dimensions();
  std::stringstream message;
  for (size_t i = 0; i < shape.size(); ++i) {
    if (current[i] != shape[i]) {
      message << "has wrong extent along dimension ";
      message << i << ", " << current[i] << " != " << shape[i];
      return NodeTestFailed(name, message.str());
    }
  }
  auto maximum = dataspace.maximum_dimensions();
  for (size_t i = 0; i < shape.size(); ++i) {
    if (maximum[i] != maximum_shape[i]) {
      message << "has wrong maximum extent along dimension ";
       message << i << ", " << maximum[i] << " != " << maximum_shape[i];
      return NodeTestFailed(name, message.str());
    }
  }
  return ::testing::AssertionSuccess();
}

static ::testing::AssertionResult NodeHasDims(
    hdf5::node::Group const & group,
    std::string const & name,
    std::vector<std::string> const & expected){
  const auto dataset = group.get_dataset(name);
  if (!dataset.attributes.exists("axes"))
    return NodeTestFailed(name, "has no 'axes' attribute");
  const auto axes_attr = dataset.attributes["axes"];
  if (axes_attr.dataspace().type() != hdf5::dataspace::Type::Scalar)
    return NodeTestFailed(name, "has non-scalar axes attribute");
  // ensure the contained datatype is a string
  if (axes_attr.datatype().get_class() != hdf5::datatype::Class::String)
    return NodeTestFailed(name, "has non-string axes attribute");
  std::string axes;
  axes_attr.read(axes);
  std::vector<std::string> dims;
  size_t ndims = std::count(axes.cbegin(), axes.cend(), ':') + 1u;
  if (ndims != expected.size())
    return NodeTestFailed(name, "has wrong number of dimensions in axes attribute");
  dims.reserve(ndims);
  {
    std::stringstream ss(axes);
    std::string dim;
    while (std::getline(ss, dim, ':')) {
      dims.push_back(dim);
    }
  }
  if (dims != expected)
    return NodeTestFailed(name, "has wrong 'axes' axes attribute");
  std::cout << "Dataset " << name << " has axes: [";
        for (auto const & dim : dims) std::cout << " " << dim;
        std::cout << " ]" << std::endl;
  return ::testing::AssertionSuccess();
}

template<class T>
static ::testing::AssertionResult NodeHasData(
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
  return passed ? ::testing::AssertionSuccess() : NodeTestFailed(name, failure_message);
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
  using da00_Writer::VariablePtrs;
  using da00_Writer::ConstantPtrs;
};


class da00_VariableConfigTestFixture : public ::testing::Test {
public:
  using da00_Writer = WriterModule::da00::da00_Writer;
};
using json = nlohmann::json;

static json createVariableJson(){
  return json::parse(R"""({
    "name": "signal",
    "unit": "counts",
    "label": "Integrated detector counts",
    "data_type": "uint64",
    "axes": ["x", "y"],
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
    ConstantJson.erase("axes");
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
    EXPECT_EQ(variable.has_dtype(), true);
    EXPECT_EQ(variable.has_axes(), true);
    EXPECT_EQ(variable.has_shape(), true);
    EXPECT_EQ(variable.has_data(), false);
    EXPECT_EQ(variable.name(), "signal");
    EXPECT_EQ(variable.unit(), "counts");
    EXPECT_EQ(variable.label(), "Integrated detector counts");
    EXPECT_EQ(variable.dtype(), da00_dtype::uint64);
    EXPECT_EQ(variable.axes(), std::vector<std::string>({"x", "y"}));
    EXPECT_EQ(variable.shape(), std::vector<hsize_t>({10, 10}));
}

static json createConstantJson(){
    return json::parse(R"""({
    "name": "x",
    "unit": "cm",
    "label": "Position along x-axis",
    "data_type": "float32",
    "axes": ["x"],
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
    ConstantJson.erase("axes");
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
    EXPECT_EQ(constant.has_dtype(), true);
    EXPECT_EQ(constant.has_axes(), true);
    EXPECT_EQ(constant.has_shape(), true);
    EXPECT_EQ(constant.has_data(), true);
    EXPECT_EQ(constant.name(), "x");
    EXPECT_EQ(constant.unit(), "cm");
    EXPECT_EQ(constant.label(), "Position along x-axis");
    EXPECT_EQ(constant.dtype(), da00_dtype::float32);
    EXPECT_EQ(constant.axes(), std::vector<std::string>({"x"}));
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
    EXPECT_EQ(constant.has_dtype(), true);
    EXPECT_EQ(constant.has_axes(), true);
    EXPECT_EQ(constant.has_shape(), true);
    EXPECT_EQ(constant.has_data(), false);
    EXPECT_EQ(constant.name(), "x");
    EXPECT_EQ(constant.unit(), "cm");
    EXPECT_EQ(constant.label(), "Position along x-axis");
    EXPECT_EQ(constant.dtype(), da00_dtype::float32);
    EXPECT_EQ(constant.axes(), std::vector<std::string>({"x"}));
    EXPECT_EQ(constant.shape(), std::vector<hsize_t>({5}));
}

static json createMinMaxSizeConstantJson(){
    return json::parse(R"""({
    "name": "x",
    "unit": "cm",
    "label": "Position along x-axis",
    "data_type": "float32",
    "axes": ["x"],
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
    EXPECT_EQ(constant.has_dtype(), true);
    EXPECT_EQ(constant.has_axes(), true);
    EXPECT_EQ(constant.has_shape(), true);
    EXPECT_EQ(constant.has_data(), true);
    EXPECT_EQ(constant.name(), "x");
    EXPECT_EQ(constant.unit(), "cm");
    EXPECT_EQ(constant.label(), "Position along x-axis");
    EXPECT_EQ(constant.dtype(), da00_dtype::float32);
    EXPECT_EQ(constant.axes(), std::vector<std::string>({"x"}));
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

template<class T>
class da00_TypeTestFixture :public ::testing::TestWithParam<std::pair<json, T>> {
  public:
  using da00_Writer = WriterModule::da00::da00_Writer;
};
using da00_DataTypeTestFixture = da00_TypeTestFixture<da00_dtype>;

TEST_P(da00_DataTypeTestFixture, DataTypeTest){
    auto input = GetParam();
    WriterModule::da00::VariableConfig constant;
    constant = input.first.dump();
    EXPECT_EQ(constant.dtype(), input.second);
}

static auto make_datatypetest_input(std::string const &type) {
    auto j = json::parse(R"""({"name": "x", "data_type": ")""" + type + R"""("})""");
    auto t = WriterModule::da00::string_to_da00_dtype(type);
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
static auto insert_variable(flatbuffers::FlatBufferBuilder & builder,
                            std::string const & name, std::string const & unit,
                            std::string const & label, std::vector<T> data,
                            const std::vector<int64_t> & shape,
                            const std::vector<std::string> & dims
                            )
{
    using namespace WriterModule::da00;
    auto name_o = builder.CreateString(name);
    auto unit_o = builder.CreateString(unit);
    auto label_o = builder.CreateString(label);
    auto source_o = builder.CreateString("test_source_name");
    auto dtype = get_dtype<T>(T());
    auto dims_o = builder.CreateVectorOfStrings(dims);
    auto shape_o = builder.CreateVector(shape);
    auto ptr = reinterpret_cast<uint8_t*>(data.data());
    auto count = data.size() * sizeof(T) / sizeof(uint8_t);
    const std::vector<uint8_t> data_span(ptr, ptr + count);
    auto data_o = builder.CreateVector<uint8_t>(data_span);
    return Createda00_Variable(builder, name_o, unit_o, label_o, source_o, dtype, dims_o, shape_o, data_o);
}

static auto make_da00_message(){
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
  auto variables = builder.CreateVector(std::vector<flatbuffers::Offset<da00_Variable>>{signal_variable, x_variable, y_variable});

  uint64_t timestamp = 19820909;
  auto data_array = Createda00_DataArray(builder, source_name, timestamp, variables);
  builder.Finish(data_array, da00_DataArrayIdentifier());
  return builder.Release(); // returns a flatbuffers::DetachedBuffer
}


static json make_da00_configuration_complete(){
  return json::parse(R"""({
    "topic": "test.topic.name",
    "source": "test_producer_name",
    "attributes" : [
        {"name": "test_attribute", "data": "test_value"}
    ],
    "variables": [
        {
          "name": "signal",
          "unit": "counts",
          "label": "Integrated counts on strip detector",
          "data_type": "uint64",
          "axes": ["x", "y"],
          "shape": [3, 3]
        }
      ],
    "constants": [
        {
          "name": "x",
          "unit": "cm",
          "label": "Binned position along x-axis",
          "data_type": "float32",
          "axes": ["x"],
          "shape": [4],
          "data": [10.0, 20.1, 30.2, 40.3]
        },
        {
          "name": "y",
          "unit": "fm",
          "label": "Position along y-axis",
          "data_type": "int8",
          "axes": ["y"],
          "shape": [3],
          "data": [9, 6, 3]
        }
      ]
  })""");
}


static json make_da00_configuration_abbreviated(){
  return json::parse(R"""({
    "topic": "test.topic.name",
    "source": "test_producer_name",
    "variables": [
        {
          "name": "signal",
          "unit": {"size": 1024},
          "label": {"size": 1024},
          "data_type": "uint64",
          "axes": ["x", "y"],
          "shape": [3, 3]
        },
        {
          "name": "signal_error",
          "unit": {"size": 1024},
          "label": {"size": 1024},
          "data_type": "float64",
          "axes": ["x", "y"],
          "shape": [3, 3]
        },
        {
          "name": "gain",
          "data_type": "uint32",
          "axes": ["x"],
          "shape": [3]
        }
      ],
    "constants": [
        {
          "name": "x",
          "unit": {"size": 1024},
          "label": {"size": 1024},
          "data_type": "float32",
          "axes": ["x"],
          "shape": [4],
          "data": {"first": 10, "last": 40.3, "size": 4}
        },
        {
          "name": "y",
          "unit": {"size": 1024},
          "label": {"size": 1024},
          "data_type": "int8",
          "axes": ["y"],
          "shape": [3],
          "data": {"first": 9, "last": 3, "size": 3}
        }
      ]
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
    EXPECT_TRUE(NodeIsValid(_group, name));
  }
  const std::vector<hsize_t> signal_shape{0, 3, 3}, x_shape{4}, y_shape{3};
  const std::vector<hsize_t> max_signal_shape{H5S_UNLIMITED, 3, 3};
  EXPECT_TRUE(NodeHasUnit(_group, "signal", "counts"));
  EXPECT_TRUE(NodeHasLabel(_group, "signal", "Integrated counts on strip detector"));
  EXPECT_TRUE(NodeHasShape(_group, "signal", signal_shape, max_signal_shape));

  const auto x_data = std::vector<float>{{10.0, 20.1, 30.2, 40.3}};
  const auto y_data = std::vector<std::int8_t>{{9, 6, 3}};
  EXPECT_TRUE(NodeHasUnit(_group, "x", "cm"));
  EXPECT_TRUE(NodeHasLabel(_group, "x", "Binned position along x-axis"));
  EXPECT_TRUE(NodeHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(NodeHasData(_group, "x", x_data));

  EXPECT_TRUE(NodeHasUnit(_group, "y", "fm"));
  EXPECT_TRUE(NodeHasLabel(_group, "y", "Position along y-axis"));
  EXPECT_TRUE(NodeHasShape(_group, "y", y_shape, y_shape));
  EXPECT_TRUE(NodeHasData(_group, "y", y_data));

  const std::vector<hsize_t> time_shape{0}, max_time_shape{H5S_UNLIMITED};
  EXPECT_TRUE(NodeHasShape(_group, "time", time_shape, max_time_shape));

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
    EXPECT_TRUE(NodeIsValid(_group, name));
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
    EXPECT_TRUE(NodeIsValid(_group, name));
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
    EXPECT_TRUE(NodeIsValid(_group, name));
  }
  const std::vector<hsize_t> signal_shape{0, 3, 3}, x_shape{4}, y_shape{3};
  const std::vector<hsize_t> max_signal_shape{H5S_UNLIMITED, 3, 3};
  const std::vector<std::string> signal_dims={"time", "x", "y"};
  EXPECT_TRUE(NodeHasUnit(_group, "signal", "counts"));
  EXPECT_TRUE(NodeHasLabel(_group, "signal", "Integrated counts on strip detector"));
  EXPECT_TRUE(NodeHasShape(_group, "signal", signal_shape, max_signal_shape));
  EXPECT_TRUE(NodeHasDims(_group, "signal", signal_dims));

  const auto x_data = std::vector<float>{{10.0, 20.1, 30.2, 40.3}};
  const auto y_data = std::vector<std::int8_t>{{9, 6, 3}};
  EXPECT_TRUE(NodeHasUnit(_group, "x", "cm"));
  EXPECT_TRUE(NodeHasLabel(_group, "x", "Binned position along x-axis"));
  EXPECT_TRUE(NodeHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(NodeHasData(_group, "x", x_data));
  EXPECT_TRUE(NodeHasUnit(_group, "y", "fm"));
  EXPECT_TRUE(NodeHasLabel(_group, "y", "Position along y-axis"));
  EXPECT_TRUE(NodeHasShape(_group, "y", y_shape, y_shape));
  EXPECT_TRUE(NodeHasData(_group, "y", y_data));
  const std::vector<hsize_t> time_shape{0}, max_time_shape{H5S_UNLIMITED};
  EXPECT_TRUE(NodeHasShape(_group, "time", time_shape, max_time_shape));
}

TEST_F(da00_WriterTestFixture, da00_WriterInitAbbreviatedConfig) {
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(make_da00_configuration_abbreviated().dump());
    writer.init_hdf(_group);
  }
  for (const auto &name : {"signal", "signal_error", "gain", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(NodeIsValid(_group, name));
  }
  const std::vector<hsize_t> x_shape{4}, y_shape{3};
  const auto x_data = std::vector<float>{{10.0, 20.1, 30.2, 40.3}};
  const auto y_data = std::vector<std::int8_t>{{9, 6, 3}};
  EXPECT_TRUE(NodeHasUnit(_group, "x", ""));
  EXPECT_TRUE(NodeHasLabel(_group, "x", ""));
  EXPECT_TRUE(NodeHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(NodeHasData(_group, "x", x_data));
  EXPECT_TRUE(NodeHasUnit(_group, "y", ""));
  EXPECT_TRUE(NodeHasLabel(_group, "y", ""));
  EXPECT_TRUE(NodeHasShape(_group, "y", y_shape, y_shape));
  EXPECT_TRUE(NodeHasData(_group, "y", y_data));
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
    EXPECT_TRUE(NodeIsValid(_group, name));
  }
  // variables are extended (but constants are not)
  const std::vector<hsize_t> signal_shape{write_count, 3, 3}, x_shape{4}, y_shape{3};
  const std::vector<hsize_t> max_signal_shape{H5S_UNLIMITED, 3, 3};
  EXPECT_TRUE(NodeHasShape(_group, "signal", signal_shape, max_signal_shape));
  EXPECT_TRUE(NodeHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(NodeHasShape(_group, "y", y_shape, y_shape));
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

TEST_F(da00_WriterTestFixture, da00_WriterFillsInAttributes) {
  auto message = FileWriter::FlatbufferMessage(_buffer.data(), _buffer.size());
  {
    auto writer = da00_WriterStandIn();
    writer.parse_config(make_da00_configuration_abbreviated().dump());
    writer.init_hdf(_group);
    writer.reopen(_group);
    constexpr int write_count{10};
    for (int i=0; i<write_count; ++i) EXPECT_NO_THROW(writer.write(message));
    EXPECT_EQ(write_count, writer.Timestamp.dataspace().size());
  }
  for (const auto &name : {"signal", "signal_error", "gain", "x", "y", "time", "cue_index", "cue_timestamp_zero"}) {
    EXPECT_TRUE(NodeIsValid(_group, name));
  }
  {
    // the FlatbufferMessage does not include "signal_error" at all
    // This should produce log messages, and fill the dataset with non-signaling
    // NaN values (since it is float64 valued)
    const auto shape = hdf5::dataspace::Simple(_group.get_dataset("signal_error").dataspace());
    std::vector<double> data(shape.size());
    _group.get_dataset("signal_error").read(data);
    for (auto const & x: data) {
      EXPECT_TRUE(std::isnan(x));
    }
  }
  {
    // the messages also do not contain "gain", which should be substituted
    // by 2^32-1 since it is uint32 valued
    const auto shape = hdf5::dataspace::Simple(_group.get_dataset("gain").dataspace());
    std::vector<std::uint32_t> data(shape.size());
    _group.get_dataset("gain").read(data);
    for (auto const & x: data) {
      EXPECT_EQ(x, (std::numeric_limits<std::uint32_t>::max)());
    }
  }
  const std::vector<hsize_t> x_shape{4}, y_shape{3};
  const auto x_data = std::vector<float>{{10.0, 20.1, 30.2, 40.3}};
  const auto y_data = std::vector<std::int8_t>{{9, 6, 3}};
  EXPECT_TRUE(NodeHasUnit(_group, "x", "cm"));
  EXPECT_TRUE(NodeHasLabel(_group, "x", "Binned position along x-axis"));
  EXPECT_TRUE(NodeHasShape(_group, "x", x_shape, x_shape));
  EXPECT_TRUE(NodeHasData(_group, "x", x_data));
  EXPECT_TRUE(NodeHasUnit(_group, "y", "fm"));
  EXPECT_TRUE(NodeHasLabel(_group, "y", "Position along y-axis"));
  EXPECT_TRUE(NodeHasShape(_group, "y", y_shape, y_shape));
  EXPECT_TRUE(NodeHasData(_group, "y", y_data));
}
