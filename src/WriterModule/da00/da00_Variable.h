#pragma once

#include "FlatbufferMessage.h"
#include "HDFFile.h"
#include "Msg.h"
#include "NeXusDataset/NeXusDataset.h"
#include "da00_Edge.h"

namespace WriterModule::da00 {

class VariableConfig {
public:
  using key_t = std::string;
  using shape_t = std::vector<hsize_t>;
  using type_t = da00_type;
  using dim_t = hdf5::Dimensions;
  using sim_t = hdf5::dataspace::Simple;
  using group_t = hdf5::node::Group;
  using variable_t = std::unique_ptr<NeXusDataset::MultiDimDatasetBase>;
  using constant_t = std::unique_ptr<hdf5::node::Dataset>;
private:
  key_t _name;
  std::optional<key_t> _unit;
  std::optional<key_t> _label;
  std::optional<type_t> _type;
  std::optional<shape_t> _shape;
  std::optional<std::vector<key_t>> _dims;
  std::optional<nlohmann::json> _data;

public:
  VariableConfig() = default;
  explicit VariableConfig(key_t name) : _name(std::move(name)) {}
  VariableConfig & operator=(std::string const & config){
    key_t def{"auto"};
    auto cfg = nlohmann::json::parse(config);
    auto is_not_def = [&](auto const & key){return cfg.contains(key) && def != cfg[key];};
    _name = cfg["name"];
    if (is_not_def("unit")) _unit = cfg["unit"];
    if (is_not_def("label")) _label = cfg["label"];
    if (is_not_def("data_type")) _type = string_to_da00_type(cfg["data_type"].get<key_t>());
    if (cfg.contains("shape")){
      auto shape = cfg["shape"].get<shape_t>();
      if (!shape.empty()) _shape = shape;
    }
    if (cfg.contains("dims")){
      auto dims = cfg["dims"].get<std::vector<key_t>>();
      if (!dims.empty()) _dims = dims;
    }
    if (cfg.contains("data")) _data = cfg["data"];
    if (!is_consistent()) {
      LOG_WARN("Inconsistent variable config for variable {}.", _name);
    }
    return *this;
  }
  explicit VariableConfig(Variable const * buffer) {
    _name = buffer->name()->str();
    if (buffer->unit()) _unit = buffer->unit()->str();
    if (buffer->label()) _label = buffer->label()->str();
    _type = dtype_to_da00_type(buffer->data_type());
    if (buffer->shape()) {
      auto shape = std::vector<hsize_t>(buffer->shape()->begin(), buffer->shape()->end());
      if (!shape.empty()) _shape = shape;
    }
    if (buffer->dims()) {
      auto dims = std::vector<std::string>();
      dims.reserve(buffer->dims()->size());
      for (const auto & dim : *buffer->dims()) {
        dims.push_back(dim->str());
      }
      if (!dims.empty()) _dims = dims;
    }
    //if (buffer->data()) _data = buffer->data()->DataAsJson();
    if (!is_consistent()) {
      LOG_WARN("Inconsistent variable config for variable {}.", _name);
    }
  }
  VariableConfig & operator=(VariableConfig const & other)= default;
  VariableConfig(VariableConfig const & other)= default;
  VariableConfig(VariableConfig && other)= default;

  [[nodiscard]] bool has_unit() const {return _unit.has_value();}
  [[nodiscard]] bool has_label() const {return _label.has_value();}
  [[nodiscard]] bool has_type() const {return _type.has_value();}
  [[nodiscard]] bool has_shape() const {return _shape.has_value();}
  [[nodiscard]] bool has_dims() const {return _dims.has_value();}
  [[nodiscard]] bool has_data() const {return _data.has_value();}
  [[nodiscard]] bool is_buildable() const {return has_type() && has_shape();}
  [[nodiscard]] auto const & name() const {return _name;}
  [[nodiscard]] auto const & unit() const {return _unit.value();}
  [[nodiscard]] auto const & label() const {return _label.value();}
  [[nodiscard]] auto const & type() const {return _type.value();}
  [[nodiscard]] auto const & shape() const {return _shape.value();}
  [[nodiscard]] auto const & dims() const {return _dims.value();}
  [[nodiscard]] auto const & json_data() const {return _data.value();}
//  template<class T>
//  [[nodiscard]] auto data() const {return json_data().get<std::vector<T>>();}
  template<class T>
  [[nodiscard]] auto data() const {
    // Allow _either_
    //     data = [1, 2, 3, ..., N],
    // or
    //     data = {min=1, max=N, size=N},
    auto cfg = EdgeConfig();
    cfg = _data.value().dump();
    return cfg.edges<T>();
  }
  void name(std::string n) {_name = std::move(n);}
  void unit(std::string u) {_unit = std::move(u);}
  void label(std::string l) {_label = std::move(l);}
  void type(type_t t) {_type = t;}
  void type(const key_t & t) {_type = string_to_da00_type(t);}
  void type(DType t) {_type = dtype_to_da00_type(t);}
  void shape(std::vector<hsize_t> s) {_shape = std::move(s);}
  void dims(std::vector<key_t> d) {_dims = std::move(d);}

  [[nodiscard]] bool is_consistent() const {
    if (has_dims() && has_shape()) {
      if (dims().size() != shape().size()) {
        LOG_WARN("Consistency check failed for variable {}. Dims and shape have different sizes.", name());
        return false;
      }
    }
    return true;
  }

  std::pair<bool, bool> update_from(const VariableConfig & other, bool force) {
    bool inconsistent{false}, changed{false};
    if (name() != other.name()) {
      LOG_DEBUG("Variable name mismatch for variable {}. Expected {}, got {}.", name(), name(), other.name());
      return std::make_pair(false, false);
    }
    if (has_unit() && other.has_unit() && unit() != other.unit()) {
      LOG_DEBUG("Unit mismatch for variable {}. Expected {}, got {}.", name(), unit(), other.unit());
      inconsistent = true;
      if (force) unit(other.unit());
    } else if (!has_unit()) {
      changed = true;
      unit(other.unit());
    }
    if (has_label() && other.has_label() && label() != other.label()) {
      LOG_DEBUG("Label mismatch for variable {}. Expected {}, got {}.", name(), label(), other.label());
      inconsistent = true;
      if (force) label(other.label());
    } else if (!has_label()) {
      changed = true;
      label(other.label());
    }
    if (has_type() && other.has_type() && type() != other.type()) {
      LOG_DEBUG("Type mismatch for variable {}. Expected {}, got {}.", name(), type(), other.type());
      inconsistent = true;
      if (force) type(other.type());
    } else if (!has_type()) {
      changed = true;
      type(other.type());
    }
    if (has_shape() && other.has_shape() && shape() != other.shape()) {
      std::stringstream ts, os;
      ts << "["; for (auto const & t : shape()) ts << t << ", "; ts << "]";
      os << "["; for (auto const & o : other.shape()) os << o << ", "; os << "]";
      LOG_DEBUG("Shape mismatch for variable {}. Expected {}, got {}.", name(), ts.str(), os.str());
      inconsistent = true;
      if (force) shape(other.shape());
    } else if (!has_shape()) {
      changed = true;
      shape(other.shape());
    }
    if (has_dims() && other.has_dims() && dims() != other.dims()) {
      std::stringstream ts, os;
      ts << "["; for (auto const & t : dims()) ts << t << ", "; ts << "]";
      os << "["; for (auto const & o : other.dims()) os << o << ", "; os << "]";
      LOG_DEBUG("Dims mismatch for variable {}. Expected {}, got {}.", name(), ts.str(), os.str());
      inconsistent = true;
      if (force) dims(other.dims());
    } else if (!has_dims()) {
      changed = false;
      dims(other.dims());
    }
    return std::make_pair(inconsistent, changed || (force && inconsistent));
  }
private:
  template<class DataType>
  auto constant_dataset(group_t const & group) const {
    auto sh = has_shape() ? shape() : dim_t{};
    auto dataset = std::make_unique<hdf5::node::Dataset>(group, name(), hdf5::datatype::create<DataType>(), sim_t(sh, sh));
    if (has_unit()) dataset->attributes.create_from("units", _unit.value());
    if (has_label()) dataset->attributes.create_from("label", _label.value());
    if (has_dims()) dataset->attributes.create_from("axes", _dims.value());
    if (has_data()) {
      LOG_DEBUG("Writing constant dataset {} from std::vector.\n", name());
      dataset->write(data<DataType>());
    }
    return dataset;
  }
  template<class DataType>
  void write_constant(constant_t & dataset, const uint8_t * data, hsize_t bytes) const {
    auto sh = shape();
    auto count = std::accumulate(sh.cbegin(), sh.cend(), 1, std::multiplies<>());
    if (count * sizeof(DataType) != bytes * sizeof(uint8_t)) {
      LOG_ERROR("Buffer size mismatch for variable {}. Expected {} bytes, got {} bytes.", name(), count * sizeof(DataType), bytes * sizeof(uint8_t));
    }
    auto array = std::vector(reinterpret_cast<const DataType *>(data), reinterpret_cast<const DataType *>(data) + count);
    LOG_DEBUG("Writing constant dataset {} from buffer via ArrayAdapter.\n", name());
    dataset->write(array);
  }
  template<class DataType>
  [[nodiscard]] variable_t variable_dataset(group_t const & group, dim_t chunk) const {
    // use the NeXusDataset MultiDimDataset to create a dataset with helpful
    // appendArray functionality (we don't use variable_t here, since we need the type information)
    using dat_t = NeXusDataset::MultiDimDataset<DataType>;
    auto sh = has_shape() ? shape() : dim_t{};
    auto dataset = std::make_unique<dat_t>(group, name(), NeXusDataset::Mode::Create, sh, chunk);
    if (has_unit()) dataset->attributes.create_from("units", _unit.value());
    if (has_label()) dataset->attributes.create_from("label", _label.value());
    if (has_dims()) dataset->attributes.create_from("axes", _dims.value());
    return dataset;
  }

  template<class DataType>
  void append_variable(variable_t & dataset, const uint8_t * data, const hdf5::Dimensions & shape) const {
    if (has_shape()) {
      const auto & my_shape = this->shape();
      if (my_shape.size() != shape.size()) {
        LOG_ERROR("Shape mismatch for variable {}. Expected {} dimensions, got {} dimensions.", name(), my_shape.size(), shape.size());
      }
      for (size_t i = 0; i < my_shape.size(); ++i) {
        if (my_shape[i] != shape[i]) {
          LOG_ERROR("Shape mismatch for variable {}. Expected shape {}, got shape {}.", name(), my_shape, shape);
        }
      }
    }
    auto count = std::accumulate(shape.cbegin(), shape.cend(), 1, std::multiplies<>());
    auto array = hdf5::ArrayAdapter<const DataType>(reinterpret_cast<const DataType *>(data), count);
    dataset->appendArray(array, shape);
  }


public:
  auto insert_constant_dataset(group_t const & group) const {
    std::map<da00_type, std::function<constant_t()>> call_map {
      {da00_type::int8, [&](){return constant_dataset<std::int8_t>(group);}},
      {da00_type::uint8, [&](){return constant_dataset<std::uint8_t>(group);}},
      {da00_type::int16, [&](){return constant_dataset<std::int16_t>(group);}},
      {da00_type::uint16, [&](){return constant_dataset<std::uint16_t>(group);}},
      {da00_type::int32, [&](){return constant_dataset<std::int32_t>(group);}},
      {da00_type::uint32, [&](){return constant_dataset<std::uint32_t>(group);}},
      {da00_type::int64, [&](){return constant_dataset<std::int64_t>(group);}},
      {da00_type::uint64, [&](){return constant_dataset<std::uint64_t>(group);}},
      {da00_type::float32, [&](){return constant_dataset<std::float_t>(group);}},
      {da00_type::float64, [&](){return constant_dataset<std::double_t>(group);}},
      {da00_type::c_string, [&](){return constant_dataset<char>(group);}}
    };
    auto dtype = has_type() ? _type.value() : da00_type::float64;
    if (dtype == da00_type::none) {
      LOG_WARN("Unknown data type for da00 constant {}. Using float64", name());
      dtype = da00_type::float64;
    }
    return call_map[dtype]();
  }
  auto write_constant_dataset(constant_t & dataset, const uint8_t * data, hsize_t count) const {
    std::map<da00_type, std::function<void()>> call_map {
        {da00_type::int8, [&](){write_constant<std::int8_t>(dataset, data, count);}},
        {da00_type::uint8, [&](){write_constant<std::uint8_t>(dataset, data, count);}},
        {da00_type::int16, [&](){write_constant<std::int16_t>(dataset, data, count);}},
        {da00_type::uint16, [&](){write_constant<std::uint16_t>(dataset, data, count);}},
        {da00_type::int32, [&](){write_constant<std::int32_t>(dataset, data, count);}},
        {da00_type::uint32, [&](){write_constant<std::uint32_t>(dataset, data, count);}},
        {da00_type::int64, [&](){write_constant<std::int64_t>(dataset, data, count);}},
        {da00_type::uint64, [&](){write_constant<std::uint64_t>(dataset, data, count);}},
        {da00_type::float32, [&](){write_constant<std::float_t>(dataset, data, count);}},
        {da00_type::float64, [&](){write_constant<std::double_t>(dataset, data, count);}},
        {da00_type::c_string, [&](){write_constant<char>(dataset, data, count);}}
    };
    auto dtype = has_type() ? _type.value() : da00_type::float64;
    if (dtype == da00_type::none) {
      LOG_WARN("Unknown data type for da00 constant {}. Using float64", name());
      dtype = da00_type::float64;
    }
    return call_map[dtype]();
  }

  auto insert_variable_dataset(group_t const & group, const dim_t& chunk_size) const {
    std::map<da00_type, std::function<variable_t()>> call_map {
      {da00_type::int8, [&](){return variable_dataset<std::int8_t>(group, chunk_size);}},
      {da00_type::uint8, [&](){return variable_dataset<std::uint8_t>(group, chunk_size);}},
      {da00_type::int16, [&](){return variable_dataset<std::int16_t>(group, chunk_size);}},
      {da00_type::uint16, [&](){return variable_dataset<std::uint16_t>(group, chunk_size);}},
      {da00_type::int32, [&](){return variable_dataset<std::int32_t>(group, chunk_size);}},
      {da00_type::uint32, [&](){return variable_dataset<std::uint32_t>(group, chunk_size);}},
      {da00_type::int64, [&](){return variable_dataset<std::int64_t>(group, chunk_size);}},
      {da00_type::uint64, [&](){return variable_dataset<std::uint64_t>(group, chunk_size);}},
      {da00_type::float32, [&](){return variable_dataset<std::float_t>(group, chunk_size);}},
      {da00_type::float64, [&](){return variable_dataset<std::double_t>(group, chunk_size);}},
      {da00_type::c_string, [&](){return variable_dataset<char>(group, chunk_size);}}
    };
    auto dtype = has_type() ? _type.value() : da00_type::float64;
    if (dtype == da00_type::none) {
      LOG_WARN("Unknown data type for da00 variable {}. Using float64", name());
      dtype = da00_type::float64;
    }
    return call_map[dtype]();
  }

  auto append_variable_dataset(variable_t & dataset, DType dtype, const uint8_t * data, const hdf5::Dimensions & shape) const {
    std::map<DType, std::function<void()>> call_map {
        {DType::int8, [&](){append_variable<std::int8_t>(dataset, data, shape);}},
        {DType::uint8, [&](){append_variable<std::uint8_t>(dataset, data, shape);}},
        {DType::int16, [&](){append_variable<std::int16_t>(dataset, data, shape);}},
        {DType::uint16, [&](){append_variable<std::uint16_t>(dataset, data, shape);}},
        {DType::int32, [&](){append_variable<std::int32_t>(dataset, data, shape);}},
        {DType::uint32, [&](){append_variable<std::uint32_t>(dataset, data, shape);}},
        {DType::int64, [&](){append_variable<std::int64_t>(dataset, data, shape);}},
        {DType::uint64, [&](){append_variable<std::uint64_t>(dataset, data, shape);}},
        {DType::float32, [&](){append_variable<std::float_t>(dataset, data, shape);}},
        {DType::float64, [&](){append_variable<std::double_t>(dataset, data, shape);}},
        {DType::c_string, [&](){append_variable<char>(dataset, data, shape);}}
    };
    if (has_type() && _type.value() != da00_type::none && dtype_to_da00_type(dtype) != _type.value()) {
      LOG_WARN("Data type mismatch for {}: (configuration={}, buffer={})", name(), _type.value(), dtype);
    }
    return call_map[dtype]();
  }

  auto variable_append(variable_t & dataset, const Variable* fb) {
    const auto data = fb->data()->Data();
    const auto axis_shape = fb->shape(); // dimension sizes
    const auto shape = hdf5::Dimensions(axis_shape->begin(), axis_shape->end());
    std::map<DType, std::function<void()>> call_map {
          {DType::int8, [&](){append_variable<std::int8_t>(dataset, data, shape);}},
          {DType::uint8, [&](){append_variable<std::uint8_t>(dataset, data, shape);}},
          {DType::int16, [&](){append_variable<std::int16_t>(dataset, data, shape);}},
          {DType::uint16, [&](){append_variable<std::uint16_t>(dataset, data, shape);}},
          {DType::int32, [&](){append_variable<std::int32_t>(dataset, data, shape);}},
          {DType::uint32, [&](){append_variable<std::uint32_t>(dataset, data, shape);}},
          {DType::int64, [&](){append_variable<std::int64_t>(dataset, data, shape);}},
          {DType::uint64, [&](){append_variable<std::uint64_t>(dataset, data, shape);}},
          {DType::float32, [&](){append_variable<std::float_t>(dataset, data, shape);}},
          {DType::float64, [&](){append_variable<std::double_t>(dataset, data, shape);}},
          {DType::c_string, [&](){append_variable<char>(dataset, data, shape);}}
    };
    if (has_type() && type() != da00_type::none && da00_type_to_dtype(type()) != fb->data_type()) {
      LOG_WARN("Data type mismatch for {}: (configuration={}, buffer={})", name(), type(), fb->data_type());
    }
    // check provided and expected axis names
    if (has_dims()) {
      const auto axis_dims = fb->dims(); // dimension _names_
      if (axis_dims->size() != dims().size()) {
        LOG_WARN("Axis dimension count mismatch for {}: (configuration={}, buffer={})",
          name(), dims().size(), axis_dims->size());
      }
      for (size_t i = 0; i < axis_dims->size(); ++i) {
        if (axis_dims->Get(i)->str() != dims()[i]) {
          LOG_WARN("Axis dimension mismatch for {} axis {}: (configuration={}, buffer={})",
            name(), i, dims()[i], axis_dims->Get(i)->str());
        }
      }
    }
    return call_map[fb->data_type()]();
  }


  // TODO FIXME Does this need to be non-const?
  [[nodiscard]] auto reopen_variable_dataset(group_t const & group) const {
    using dat_t = NeXusDataset::MultiDimDatasetBase;
    if (!group.has_dataset(name())) {
      std::stringstream ss;
      ss << group.link().path();
      LOG_ERROR("Trying to reopen variable dataset {}, but it doesn't exist under {}", name(), ss.str());
    }
    return std::make_unique<dat_t>(group, name(), NeXusDataset::Mode::Open);
  }

  [[nodiscard]] auto reopen_constant_dataset(group_t & group) const {
    if (!group.has_dataset(name())) {
      std::stringstream ss;
      ss << group.link().path();
      LOG_ERROR("Trying to reopen constant dataset {}, but it doesn't exist under {}", name(), ss.str());
    }
    return std::make_unique<hdf5::node::Dataset>(group.get_dataset(name()));
  }

  template<class T>
  bool compare_data(const std::vector<T> & other) const {
    auto our = data<T>();
    if (our.size() != other.size()) return false;
    for (size_t i = 0; i < our.size(); ++i){
      if (our[i] != other[i]) return false;
    }
    return true;
  }

  template<class Dataset>
  void update_dataset(Dataset & dataset) const {
    if (has_unit()) {
      if (dataset->attributes.exists("units")) {
        std::string var_units;
        dataset->attributes["units"].read(var_units);
        if (var_units != _unit.value()) {
          LOG_ERROR("Unit mismatch for dataset {}. (new={}, was={})", name(), _unit.value(), var_units);
          dataset->attributes["units"].write(unit());
        }
      } else {
        dataset->attributes.create_from("units", _unit.value());
      }
    }
    if (has_label()) {
      if (dataset->attributes.exists("label")) {
        std::string var_label;
        dataset->attributes["label"].read(var_label);
        if (var_label != _label.value()) {
          LOG_ERROR("Label mismatch for dataset {}. (new={}, was={})", name(), _label.value(), var_label);
          dataset->attributes["label"].write(label());
        }
      } else {
        dataset->attributes.create_from("label", _label.value());
      }
    }
    if (has_dims()) {
      if (dataset->attributes.exists("axes")) {
        // Why is the h5cpp way of doing this complicated,
        // when HighFive can resize the vector internally?
        auto ax = dataset->attributes["axes"];
        auto ds = ax.dataspace();
        std::vector<std::string> var_axes(ds.size());
        ax.read(var_axes);
        if (var_axes != dims()) {
          LOG_ERROR("Axes mismatch for dataset {}. (new={}, was={})", name(),
                    dims(), var_axes);
          dataset->attributes["axes"].write(dims());
        }
      } else {
        dataset->attributes.create_from("axes", _dims.value());
      }
    }
  }
  void update_variable(variable_t & variable) const {
    using namespace hdf5::dataspace;
    update_dataset(variable);
    if (has_shape()) {
      auto sh = shape();
      sh.insert(sh.begin(), 1);
      auto ds = variable->dataspace();
      bool resize{false};
      if (ds.type() == Type::Scalar) {
        LOG_ERROR("Variable dataset {} has non-scalar dataspace.", name());
        resize = true;
      } else {
        auto simple = Simple(ds);
        if (simple.rank() != sh.size()) {
          LOG_ERROR("Variable dataset {} has different rank than new config.", name());
          resize = true;
        } else {
          sh[0] = simple.current_dimensions()[0];
          if (simple.current_dimensions() != sh) {
            LOG_ERROR("Variable dataset {} has different shape than new config.", name());
            resize = true;
          }
        }
      }
      if (resize) {
        variable->resize(sh);
      }
    }
  }
  void update_constant(constant_t & constant) const {
    using namespace hdf5::dataspace;
    update_dataset(constant);
    if (has_shape()) {
      auto ds = constant->dataspace();
      bool resize{false};
      bool replace{false};
      if (ds.type() == Type::Scalar && !shape().empty()) {
        LOG_ERROR("Constant dataset {} has scalar dataspace but new config shape is not scalar.", name());
        replace = true;
        // somehow replace the dataspace in teh dataset?
      }
      if (ds.type() == Type::Simple && Simple(ds).current_dimensions() != shape()) {
        LOG_ERROR("Constant dataset {} has different shape than new config.", name());
        resize = true;
        auto max_size = Simple(ds).maximum_dimensions();
        auto max_count = std::accumulate(max_size.cbegin(), max_size.cend(), 1, std::multiplies<>());
        auto count = std::accumulate(shape().cbegin(), shape().cend(), 1, std::multiplies<>());
        if (count > max_count) {
          LOG_ERROR("Constant dataset {} too small maximum size {} < {}.", name(), max_count, count);
          // somehow replace the dataspace
          replace = true;
        }
      }
      if (resize || replace) {
        auto dataspace = Simple(shape(), shape());
        auto datatype = constant->datatype();
        auto link = constant->link();
        auto attrs = constant->attributes;
        hdf5::node::remove(*constant);
        auto dataset = std::make_unique<hdf5::node::Dataset>(link.parent(), link.path(), datatype, dataspace);
        constant = std::move(dataset);
      }
      // if (replace || resize) {
      //   auto nds = constant->dataspace();
      //   if (nds.type() == Type::Scalar) {
      //     LOG_ERROR("Constant dataset {} still has scalar dataspace.", name());
      //     throw std::runtime_error("Resizing will fail.");
      //   }
      //   auto max_shape = Simple(nds).maximum_dimensions();
      //   auto max_count = std::accumulate(max_shape.cbegin(), max_shape.cend(), 1, std::multiplies<>());
      //   auto count = std::accumulate(shape().cbegin(), shape().cend(), 1, std::multiplies<>());
      //   if (count > max_count) {
      //     LOG_ERROR("Constant dataset {} too small maximum size {} < {}.", name(), max_count, count);
      //     throw std::runtime_error("Resizing will fail.");
      //   }
      //
      //   // resize only ever works for chunked dataspaces but our 'constant'
      //   // datasets only ever have fully contiguos dataspaces so this always
      //   // throws an error?
      //   constant->resize(shape());
      // }
    }
  }
  template<class T>
  void update_constant(constant_t & constant, const std::vector<T> & data) const {
    update_constant(constant);
    std::vector<T> var_data;
    constant->read(var_data);
    if (var_data.size() != data.size()) {
      LOG_ERROR("Constant dataset {} has different size than new data.", name());
    }
    bool same{true};
    if constexpr (std::is_integral_v<T>) {
      same = var_data == data;
    } else {
      for (size_t i=0; i<data.size(); ++i) {
        auto diff = var_data[i] - data[i];
        auto total = var_data[i] + data[i];
        same &= std::abs(diff / total) < 1e-6;
      }
    }
    if (!same) {
      LOG_ERROR("Constant dataset {} has different data than new data.", name());
      constant->write(data);
    }
  }

};

} // namespace WriterModule::da00


template<> struct fmt::formatter<WriterModule::da00::VariableConfig> {
  template<class ParseContext>
  constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }
  template<class FormatContext>
  auto format(const WriterModule::da00::VariableConfig &c, FormatContext &ctx) {
    auto unit = c.has_unit() ? c.unit() : "none";
    auto label = c.has_label() ? c.label() : "none";
    auto type = c.has_type() ? c.type() : WriterModule::da00::da00_type::none;
    auto shape = c.has_shape() ? c.shape() : std::vector<hsize_t>{};
    auto dims = c.has_dims() ? c.dims() : std::vector<std::string>{};
    auto data = c.has_data() ? c.json_data() : nlohmann::json{};
    return format_to(ctx.out(),
       "VariableConfig(name={}, unit={}, label={}, type={}, shape={}, dims={}, data={})",
       c.name(), unit, label, type, shape, dims, data);
  }
};
