#pragma once

#include "FlatbufferMessage.h"
#include "HDFFile.h"
#include "Msg.h"
#include "NeXusDataset/NeXusDataset.h"
#include "da00_Edge.h"
#include "da00_String.h"

namespace WriterModule::da00 {

template <typename T>
std::unique_ptr<hdf5::node::ChunkedDataset>
create_chunked_dataset(hdf5::node::Group const &parent, std::string const &name,
                       hdf5::Dimensions shape, hdf5::Dimensions chunk_size) {
  shape.insert(shape.begin(), 0);
  hdf5::Dimensions max_size(shape.size(), hdf5::dataspace::Simple::unlimited);

  if (chunk_size.empty()) {
    LOG_WARN("No chunk size given. Using the default value 1024.");
    chunk_size.emplace_back(1024);
  }
  if (chunk_size.size() == shape.size()) {
    // All normal - nothing to do
  } else if (chunk_size.size() == 1 && shape.size() > 1) {
    auto elements_per_row =
        std::accumulate(std::next(shape.begin()), shape.end(), 1,
                        [](auto a, auto b) { return a * b; });
    auto num_rows = chunk_size[0] / elements_per_row;
    if (num_rows == 0) {
      num_rows = 1;
    }
    chunk_size = shape;
    chunk_size[0] = num_rows;
  } else {
    LOG_ERROR("Unable to reconcile a data shape with {} dimensions "
              "and chunk size with {} dimensions. Using default "
              "values.",
              shape.size(), chunk_size.size());
    chunk_size = shape;
    chunk_size[0] = 1024;
  }
  return std::make_unique<hdf5::node::ChunkedDataset>(
      parent, name, hdf5::datatype::create<T>(),
      hdf5::dataspace::Simple(shape, max_size), chunk_size);
}

template <typename T>
void appendArray(hdf5::node::Dataset &dataset, T const &new_data,
                 hdf5::Dimensions shape) {
  auto current_extent =
      hdf5::dataspace::Simple(dataset.dataspace()).current_dimensions();
  hdf5::Dimensions origin(current_extent.size(), 0);
  origin[0] = current_extent[0];
  ++current_extent[0];
  shape.insert(shape.begin(), 1);
  if (shape.size() != current_extent.size()) {
    LOG_ERROR("Data has {} dimension(s) and dataset has {} (+1) dimensions.",
              shape.size() - 1, current_extent.size() - 1);
    throw std::runtime_error(
        "Rank (dimensions) of data to be written is wrong.");
  }
  for (size_t i = 1; i < shape.size(); i++) {
    if (shape[i] > current_extent[i]) {
      LOG_WARN("Dimension {} of new data is larger than that of the "
               "dataset. Extending dataset.",
               i - 1);
      current_extent[i] = shape[i];
    } else if (shape[i] < current_extent[i]) {
      LOG_WARN("Dimension {} of new data is smaller than that of "
               "the dataset. Using 0 as a filler.",
               i - 1);
    }
  }
  dataset.extent(current_extent);
  hdf5::dataspace::Hyperslab selection{{origin}, {shape}};
  dataset.write(new_data, selection);
}

class VariableConfig {
public:
  using VariableDataset = std::unique_ptr<hdf5::node::Dataset>;
  using ConstantDataset = std::unique_ptr<hdf5::node::Dataset>;

  VariableConfig() = default;
  explicit VariableConfig(std::string name) : name_(std::move(name)) {}
  VariableConfig &operator=(std::string const &config) {
    auto json_config = nlohmann::json::parse(config);
    name_ = json_config["name"];
    if (json_config.contains("unit"))
      unit_ = StringConfig(json_config["unit"]);
    if (json_config.contains("label"))
      label_ = StringConfig(json_config["label"]);
    if (json_config.contains("source"))
      source_ = StringConfig(json_config["source"]);
    if (json_config.contains("data_type"))
      dtype_ =
          string_to_da00_dtype(json_config["data_type"].get<std::string>());
    if (json_config.contains("shape")) {
      auto shape = json_config["shape"].get<std::vector<hsize_t>>();
      if (!shape.empty()) {
        shape_ = shape;
      }
    }
    if (json_config.contains("axes")) {
      auto axes = json_config["axes"].get<std::vector<std::string>>();
      if (!axes.empty()) {
        axes_ = axes;
      }
    }
    if (json_config.contains("data"))
      data_ = json_config["data"];
    if (!dtype_.has_value() && data_.has_value()) {
      dtype_ = guess_dtype(data_.value());
      LOG_ERROR("No data type specified for variable {}. Guessing type {}.",
                name_, dtype_.value());
    }
    if (!shape_.has_value() && data_.has_value()) {
      shape_ = get_shape(data_.value());
      LOG_ERROR("No shape specified for variable {}. Guessing shape {}.", name_,
                shape_.value());
    }
    if (!is_consistent()) {
      LOG_WARN("Inconsistent variable config for variable {}.", name_);
    }
    return *this;
  }
  explicit VariableConfig(da00_Variable const *buffer) {
    name_ = buffer->name()->str();
    if (buffer->unit())
      unit_ = StringConfig(buffer->unit()->str());
    if (buffer->label())
      label_ = StringConfig(buffer->label()->str());
    dtype_ = buffer->data_type();
    if (buffer->shape()) {
      auto shape = std::vector<hsize_t>(buffer->shape()->begin(),
                                        buffer->shape()->end());
      if (!shape.empty())
        shape_ = shape;
    }
    if (buffer->axes()) {
      auto dims = std::vector<std::string>();
      dims.reserve(buffer->axes()->size());
      for (const auto &dim : *buffer->axes()) {
        dims.push_back(dim->str());
      }
      if (!dims.empty())
        axes_ = dims;
    }
    if (!is_consistent()) {
      LOG_WARN("Inconsistent variable config for variable {}.", name_);
    }
  }
  VariableConfig &operator=(VariableConfig const &other) = default;
  VariableConfig(VariableConfig const &other) = default;
  VariableConfig(VariableConfig &&other) = default;

  [[nodiscard]] bool has_unit() const { return unit_.has_value(); }
  [[nodiscard]] bool has_label() const { return label_.has_value(); }
  [[nodiscard]] bool has_source() const { return source_.has_value(); }
  [[nodiscard]] bool has_dtype() const { return dtype_.has_value(); }
  [[nodiscard]] bool has_shape() const { return shape_.has_value(); }
  [[nodiscard]] bool has_axes() const { return axes_.has_value(); }
  [[nodiscard]] bool has_data() const { return data_.has_value(); }
  [[nodiscard]] auto const &name() const { return name_; }
  [[nodiscard]] auto const &unit() const { return unit_.value(); }
  [[nodiscard]] auto const &label() const { return label_.value(); }
  [[nodiscard]] auto const &source() const { return source_.value(); }
  [[nodiscard]] auto const &dtype() const { return dtype_.value(); }
  [[nodiscard]] auto const &shape() const { return shape_.value(); }
  [[nodiscard]] auto const &axes() const { return axes_.value(); }
  [[nodiscard]] auto colsepaxes() const {
    std::stringstream ss;
    auto axes = axes_.value();
    if (axes.empty())
      return ss.str();
    std::string sep = ":";
    for (const auto &ax : axes)
      ss << ax << sep;
    auto out = ss.str();
    return out.substr(0, out.size() - sep.size());
  }
  [[nodiscard]] auto const &json_data() const { return data_.value(); }

  template <class T> [[nodiscard]] auto data() const {
    // Allow _either_
    //     data = [1, 2, 3, ..., N],
    // or
    //     data = {min=1, max=N, size=N},
    EdgeConfig cfg = EdgeConfig();
    cfg = data_.value().dump();
    return cfg.edges<T>();
  }
  void name(std::string name) { name_ = std::move(name); }
  void unit(std::string unit) { unit_ = StringConfig(std::move(unit)); }
  void label(std::string label) { label_ = StringConfig(std::move(label)); }
  void source(std::string source) { source_ = StringConfig(std::move(source)); }
  void dtype(da00_dtype type) { dtype_ = type; }
  void dtype(const std::string &type) { dtype_ = string_to_da00_dtype(type); }
  void shape(std::vector<hsize_t> shape) { shape_ = std::move(shape); }
  void dims(std::vector<std::string> dims) { axes_ = std::move(dims); }

  [[nodiscard]] bool is_consistent() const {
    if (has_axes() && has_shape() && axes().size() != shape().size()) {
      LOG_WARN("Consistency check failed for variable {}. Dims and shape "
               "have different sizes.",
               name());
      return false;
    }
    return true;
  }

  std::pair<bool, bool> update_from(const VariableConfig &other, bool force) {
    bool inconsistent{false};
    bool changed{false};
    if (name() != other.name()) {
      LOG_DEBUG("Variable name mismatch for variable {}. Expected {}, got {}.",
                name(), name(), other.name());
      return std::make_pair(false, false);
    }
    if (has_unit() && other.has_unit()) {
      auto [first, second] = unit_.value().update_from(other.unit(), force);
      inconsistent |= first;
      changed |= second;
    } else if (other.has_unit()) {
      changed = true;
      unit_ = other.unit();
    }
    if (has_label() && other.has_label()) {
      auto [first, second] = label_.value().update_from(other.label(), force);
      inconsistent |= first;
      changed |= second;
    } else if (other.has_label()) {
      changed = true;
      label_ = other.label();
    }
    if (has_source() && other.has_source()) {
      auto [first, second] = source_.value().update_from(other.source(), force);
      inconsistent |= first;
      changed |= second;
    } else if (other.has_source()) {
      changed = true;
      source_ = other.source();
    }
    if (has_dtype() && other.has_dtype() && dtype() != other.dtype()) {
      LOG_DEBUG("Data type mismatch for variable {}. Expected {}, got {}.",
                name(), dtype(), other.dtype());
      inconsistent = true;
      if (force)
        dtype(other.dtype());
    } else if (!has_dtype() && other.has_dtype()) {
      changed = true;
      dtype(other.dtype());
    }
    if (has_shape() && other.has_shape() && shape() != other.shape()) {
      std::stringstream ts;
      std::stringstream os;
      ts << "[";
      for (auto const &t : shape())
        ts << t << ", ";
      ts << "]";
      os << "[";
      for (auto const &o : other.shape())
        os << o << ", ";
      os << "]";
      LOG_DEBUG("Shape mismatch for variable {}. Expected {}, got {}.", name(),
                ts.str(), os.str());
      inconsistent = true;
      if (force)
        shape(other.shape());
    } else if (!has_shape() && other.has_shape()) {
      changed = true;
      shape(other.shape());
    }
    if (has_axes() && other.has_axes() && axes() != other.axes()) {
      std::stringstream ts;
      std::stringstream os;
      ts << "[";
      for (auto const &t : axes())
        ts << t << ", ";
      ts << "]";
      os << "[";
      for (auto const &o : other.axes())
        os << o << ", ";
      os << "]";
      LOG_DEBUG("Dims mismatch for variable {}. Expected {}, got {}.", name(),
                ts.str(), os.str());
      inconsistent = true;
      if (force)
        dims(other.axes());
    } else if (!has_axes() && other.has_axes()) {
      changed = false;
      dims(other.axes());
    }
    return std::make_pair(inconsistent, changed || (force && inconsistent));
  }

  template <class DataType>
  auto constant_dataset(hdf5::node::Group const &group) const {
    auto initial_shape = has_shape() ? shape() : hdf5::Dimensions{};
    auto dataset = std::make_unique<hdf5::node::Dataset>(
        group, name(), hdf5::datatype::create<DataType>(),
        hdf5::dataspace::Simple(initial_shape, initial_shape));
    add_dataset_attributes(dataset, has_axes() ? colsepaxes() : std::string());
    if (has_data())
      dataset->write(data<DataType>());
    return dataset;
  }
  template <class DataType>
  void write_constant(ConstantDataset const &dataset, const uint8_t *data,
                      hsize_t bytes) const {
    auto count = std::accumulate(shape().cbegin(), shape().cend(), 1,
                                 std::multiplies<>());
    if (count * sizeof(DataType) != bytes * sizeof(uint8_t)) {
      LOG_ERROR("Buffer size mismatch for variable {}. Expected {} bytes, got "
                "{} bytes.",
                name(), count * sizeof(DataType), bytes * sizeof(uint8_t));
    }
    // ArrayAdapter caused a segfault when writing. Copying the data to a vector
    // first works.
    auto ptr = reinterpret_cast<const DataType *>(data);
    auto array = std::vector(ptr, ptr + count);
    dataset->write(array);
  }

  template <class DataType>
  [[nodiscard]] VariableDataset
  variable_dataset(hdf5::node::Group const &group, hdf5::Dimensions chunk,
                   [[maybe_unused]] const bool fixed = true) const {
    auto initial_shape = has_shape() ? shape() : hdf5::Dimensions{};
    auto dataset =
        create_chunked_dataset<DataType>(group, name(), initial_shape, chunk);
    std::string axes_spec;
    if (has_axes())
      axes_spec = "time:" + colsepaxes();
    add_dataset_attributes(dataset, axes_spec);
    return dataset;
  }

  template <class DataType>
  void append_variable(VariableDataset const &dataset, const uint8_t *data,
                       const hdf5::Dimensions &shape) const {
    if (has_shape()) {
      const auto &my_shape = this->shape();
      if (my_shape.size() != shape.size()) {
        LOG_ERROR("Shape mismatch for variable {}. Expected {} dimensions, got "
                  "{} dimensions.",
                  name(), my_shape.size(), shape.size());
      }
      for (size_t i = 0; i < my_shape.size(); ++i) {
        if (my_shape[i] != shape[i]) {
          LOG_ERROR("Shape mismatch for variable {}. Expected shape {}, got "
                    "shape {}.",
                    name(), my_shape, shape);
        }
      }
    }
    auto count =
        std::accumulate(shape.cbegin(), shape.cend(), 1, std::multiplies<>());
    auto array = hdf5::ArrayAdapter<const DataType>(
        reinterpret_cast<const DataType *>(data), count);
    appendArray(*dataset, array, shape);
  }

  template <class DataType>
  void append_missing_variable(VariableDataset const &dataset) const {
    auto upper_bound = std::numeric_limits<DataType>::has_quiet_NaN
                           ? std::numeric_limits<DataType>::quiet_NaN()
                           : (std::numeric_limits<DataType>::max)();
    auto shape = this->shape();
    auto count =
        std::accumulate(shape.cbegin(), shape.cend(), 1, std::multiplies<>());
    std::vector<DataType> missing(count, upper_bound);
    appendArray(*dataset, missing, shape);
  }

  std::unique_ptr<hdf5::node::Dataset>
  insert_constant_dataset(hdf5::node::Group const &group) const {
    std::map<da00_dtype, std::function<ConstantDataset()>> call_map{
        {da00_dtype::int8,
         [&]() { return constant_dataset<std::int8_t>(group); }},
        {da00_dtype::uint8,
         [&]() { return constant_dataset<std::uint8_t>(group); }},
        {da00_dtype::int16,
         [&]() { return constant_dataset<std::int16_t>(group); }},
        {da00_dtype::uint16,
         [&]() { return constant_dataset<std::uint16_t>(group); }},
        {da00_dtype::int32,
         [&]() { return constant_dataset<std::int32_t>(group); }},
        {da00_dtype::uint32,
         [&]() { return constant_dataset<std::uint32_t>(group); }},
        {da00_dtype::int64,
         [&]() { return constant_dataset<std::int64_t>(group); }},
        {da00_dtype::uint64,
         [&]() { return constant_dataset<std::uint64_t>(group); }},
        {da00_dtype::float32,
         [&]() { return constant_dataset<std::float_t>(group); }},
        {da00_dtype::float64,
         [&]() { return constant_dataset<std::double_t>(group); }},
        {da00_dtype::c_string,
         [&]() { return constant_dataset<char>(group); }}};
    auto dtype = has_dtype() ? dtype_.value() : da00_dtype::float64;
    return call_map[dtype]();
  }

  void write_constant_dataset(ConstantDataset &dataset, const uint8_t *data,
                              hsize_t count) const {
    std::map<da00_dtype, std::function<void()>> call_map{
        {da00_dtype::int8,
         [&]() { write_constant<std::int8_t>(dataset, data, count); }},
        {da00_dtype::uint8,
         [&]() { write_constant<std::uint8_t>(dataset, data, count); }},
        {da00_dtype::int16,
         [&]() { write_constant<std::int16_t>(dataset, data, count); }},
        {da00_dtype::uint16,
         [&]() { write_constant<std::uint16_t>(dataset, data, count); }},
        {da00_dtype::int32,
         [&]() { write_constant<std::int32_t>(dataset, data, count); }},
        {da00_dtype::uint32,
         [&]() { write_constant<std::uint32_t>(dataset, data, count); }},
        {da00_dtype::int64,
         [&]() { write_constant<std::int64_t>(dataset, data, count); }},
        {da00_dtype::uint64,
         [&]() { write_constant<std::uint64_t>(dataset, data, count); }},
        {da00_dtype::float32,
         [&]() { write_constant<std::float_t>(dataset, data, count); }},
        {da00_dtype::float64,
         [&]() { write_constant<std::double_t>(dataset, data, count); }},
        {da00_dtype::c_string,
         [&]() { write_constant<char>(dataset, data, count); }}};
    auto dtype = has_dtype() ? dtype_.value() : da00_dtype::float64;
    call_map[dtype]();
  }

  std::unique_ptr<hdf5::node::Dataset>
  insert_variable_dataset(hdf5::node::Group const &group,
                          const hdf5::Dimensions &chunk_size) const {
    std::map<da00_dtype, std::function<VariableDataset()>> call_map{
        {da00_dtype::int8,
         [&]() { return variable_dataset<std::int8_t>(group, chunk_size); }},
        {da00_dtype::uint8,
         [&]() { return variable_dataset<std::uint8_t>(group, chunk_size); }},
        {da00_dtype::int16,
         [&]() { return variable_dataset<std::int16_t>(group, chunk_size); }},
        {da00_dtype::uint16,
         [&]() { return variable_dataset<std::uint16_t>(group, chunk_size); }},
        {da00_dtype::int32,
         [&]() { return variable_dataset<std::int32_t>(group, chunk_size); }},
        {da00_dtype::uint32,
         [&]() { return variable_dataset<std::uint32_t>(group, chunk_size); }},
        {da00_dtype::int64,
         [&]() { return variable_dataset<std::int64_t>(group, chunk_size); }},
        {da00_dtype::uint64,
         [&]() { return variable_dataset<std::uint64_t>(group, chunk_size); }},
        {da00_dtype::float32,
         [&]() { return variable_dataset<std::float_t>(group, chunk_size); }},
        {da00_dtype::float64,
         [&]() { return variable_dataset<std::double_t>(group, chunk_size); }},
        {da00_dtype::c_string,
         [&]() { return variable_dataset<char>(group, chunk_size); }}};
    auto dtype = has_dtype() ? dtype_.value() : da00_dtype::float64;
    return call_map[dtype]();
  }

  void variable_append(VariableDataset &dataset,
                       const da00_Variable *fb) const {
    const auto data = fb->data()->Data();
    const auto axis_shape = fb->shape(); // dimension sizes
    const auto shape = hdf5::Dimensions(axis_shape->begin(), axis_shape->end());
    std::map<da00_dtype, std::function<void()>> call_map{
        {da00_dtype::int8,
         [&]() { append_variable<std::int8_t>(dataset, data, shape); }},
        {da00_dtype::uint8,
         [&]() { append_variable<std::uint8_t>(dataset, data, shape); }},
        {da00_dtype::int16,
         [&]() { append_variable<std::int16_t>(dataset, data, shape); }},
        {da00_dtype::uint16,
         [&]() { append_variable<std::uint16_t>(dataset, data, shape); }},
        {da00_dtype::int32,
         [&]() { append_variable<std::int32_t>(dataset, data, shape); }},
        {da00_dtype::uint32,
         [&]() { append_variable<std::uint32_t>(dataset, data, shape); }},
        {da00_dtype::int64,
         [&]() { append_variable<std::int64_t>(dataset, data, shape); }},
        {da00_dtype::uint64,
         [&]() { append_variable<std::uint64_t>(dataset, data, shape); }},
        {da00_dtype::float32,
         [&]() { append_variable<std::float_t>(dataset, data, shape); }},
        {da00_dtype::float64,
         [&]() { append_variable<std::double_t>(dataset, data, shape); }},
        {da00_dtype::c_string,
         [&]() { append_variable<char>(dataset, data, shape); }}};
    if (has_dtype() && dtype() != fb->data_type()) {
      LOG_WARN("Data type mismatch for {}: (configuration={}, buffer={})",
               name(), dtype(), fb->data_type());
      // TODO: return early? If the data doesn't match then it will crash?
    }
    // check provided and expected axis names
    if (has_axes()) {
      const auto axis_dims = fb->axes(); // dimension _names_
      if (axis_dims->size() != axes().size()) {
        LOG_WARN("Axis dimension count mismatch for {}: (configuration={}, "
                 "buffer={})",
                 name(), axes().size(), axis_dims->size());
      }
      for (size_t i = 0; i < axis_dims->size(); ++i) {
        if (axis_dims->Get(i)->str() != axes()[i]) {
          LOG_WARN("Axis dimension mismatch for {} axis {}: (configuration={}, "
                   "buffer={})",
                   name(), i, axes()[i], axis_dims->Get(i)->str());
        }
      }
    }
    call_map[fb->data_type()]();
  }

  void variable_append_missing(VariableDataset &dataset) const {
    std::map<da00_dtype, std::function<void()>> call_map{
        {da00_dtype::int8,
         [&]() { append_missing_variable<std::int8_t>(dataset); }},
        {da00_dtype::uint8,
         [&]() { append_missing_variable<std::uint8_t>(dataset); }},
        {da00_dtype::int16,
         [&]() { append_missing_variable<std::int16_t>(dataset); }},
        {da00_dtype::uint16,
         [&]() { append_missing_variable<std::uint16_t>(dataset); }},
        {da00_dtype::int32,
         [&]() { append_missing_variable<std::int32_t>(dataset); }},
        {da00_dtype::uint32,
         [&]() { append_missing_variable<std::uint32_t>(dataset); }},
        {da00_dtype::int64,
         [&]() { append_missing_variable<std::int64_t>(dataset); }},
        {da00_dtype::uint64,
         [&]() { append_missing_variable<std::uint64_t>(dataset); }},
        {da00_dtype::float32,
         [&]() { append_missing_variable<std::float_t>(dataset); }},
        {da00_dtype::float64,
         [&]() { append_missing_variable<std::double_t>(dataset); }},
        {da00_dtype::c_string,
         [&]() { append_missing_variable<char>(dataset); }}};
    if (!has_dtype()) {
      LOG_ERROR("Can not append missing data for {} without data_type!",
                name());
    }
    call_map[dtype()]();
  }

  [[nodiscard]] std::unique_ptr<hdf5::node::Dataset>
  reopen_variable_dataset(hdf5::node::Group const &group) const {
    if (!group.has_dataset(name())) {
      std::stringstream ss;
      ss << group.link().path();
      LOG_ERROR(
          "Trying to reopen variable dataset {}, but it doesn't exist under {}",
          name(), ss.str());
    }
    return std::make_unique<hdf5::node::Dataset>(group.get_dataset(name()));
  }

  [[nodiscard]] std::unique_ptr<hdf5::node::Dataset>
  reopen_constant_dataset(hdf5::node::Group &group) const {
    if (!group.has_dataset(name())) {
      std::stringstream ss;
      ss << group.link().path();
      LOG_ERROR(
          "Trying to reopen constant dataset {}, but it doesn't exist under {}",
          name(), ss.str());
    }
    return std::make_unique<hdf5::node::Dataset>(group.get_dataset(name()));
  }

  template <class T> bool compare_data(const std::vector<T> &other) const {
    auto our = data<T>();
    if (our.size() != other.size())
      return false;
    for (size_t i = 0; i < our.size(); ++i) {
      if (our[i] != other[i])
        return false;
    }
    return true;
  }

  template <class Dataset>
  void
  update_dataset_attributes(Dataset &dataset,
                            const bool SingeWriterMultipleReader = true) const {
    auto do_update = [&](const std::optional<StringConfig> &obj,
                         const std::string &attr) {
      if (obj.has_value() && obj.value().has_value()) {
        if (dataset->attributes.exists(attr)) {
          std::string var_obj;
          dataset->attributes[attr].read(var_obj);
          const auto last = var_obj.find('\0');
          if (last != std::string::npos)
            var_obj.resize(last);
          if (var_obj != obj.value().value()) {
            LOG_DEBUG("Mismatch for dataset {}. (new={}, was={})", name(),
                      obj.value(), var_obj);
            auto datatype = dataset->attributes[attr].datatype();
            dataset->attributes[attr].write(obj.value().value(), datatype);
          }
        } else if (!SingeWriterMultipleReader) {
          // only allowed to _create_ attributes if not SWMR-mode
          obj.value().insert_attribute(dataset, attr);
        }
      }
    };
    do_update(unit_, "units");
    do_update(label_, "long_name");
    do_update(source_, "source");
    if (has_axes()) {
      if (dataset->attributes.exists("axes")) {
        // @axes is a colon delimited list of axis names
        std::string var_axes;
        dataset->attributes["axes"].read(var_axes);
        const auto last = var_axes.find('\0');
        if (last != std::string::npos)
          var_axes.resize(last);
        if (var_axes != colsepaxes()) {
          LOG_DEBUG("Axes mismatch for dataset {}. (new={}, was={})", name(),
                    axes(), var_axes);
          dataset->attributes["axes"].write(
              colsepaxes(), dataset->attributes["axes"].datatype());
        }
      } else if (!SingeWriterMultipleReader) {
        // like StringConfig, use fixed-size string attributes
        auto to_write = colsepaxes();
        auto datatype = hdf5::datatype::String::fixed(to_write.size());
        auto attr = dataset->attributes.create("axes", datatype,
                                               hdf5::dataspace::Scalar());
        attr.write(to_write, datatype);
      }
    }
  }
  void update_variable(VariableDataset &variable,
                       bool swmr_enabled = true) const {
    using namespace hdf5::dataspace;
    update_dataset_attributes(variable, swmr_enabled);
    if (has_shape()) {
      // TODO: continue fixing names from here downwards
      auto sh = shape();
      sh.insert(sh.begin(), 1);
      auto ds = variable->dataspace();
      bool resize{false};
      if (ds.type() == Type::Scalar) {
        LOG_ERROR("Variable dataset {} has scalar dataspace.", name());
        // resizing never allowed
      } else {
        auto simple = Simple(ds);
        if (simple.rank() != sh.size()) {
          LOG_ERROR("Variable dataset {} has different rank than new config.",
                    name());
          // we can never change the rank of an existing dataset
        } else {
          sh[0] = simple.current_dimensions()[0];
          if (simple.current_dimensions() != sh) {
            LOG_WARN("Variable dataset {} has different shape than new config.",
                     name());
            resize = true;
          }
          if (resize) {
            auto max_size = simple.maximum_dimensions();
            for (size_t i = 0; i < sh.size(); ++i) {
              if (sh[i] > max_size[i]) {
                LOG_ERROR("Variable dataset {} too small along {}, max {} <  "
                          "request {}.",
                          name(), i, max_size[i], sh[i]);
                // we can never change the maximum size of an existing dataset
                resize = false;
              }
            }
          }
        }
      }
      // if resizing is true, the new shape fits inside of the maximum extent
      // of the dataset -- which is allowed in SWMR-mode and non-SMWR-mode.
      if (resize) {
        variable->resize(sh);
      }
    }
  }
  void update_constant(ConstantDataset &constant,
                       const bool swmr_enabled = true) const {
    using namespace hdf5::dataspace;
    update_dataset_attributes(constant, swmr_enabled);
    if (has_shape()) {
      auto ds = constant->dataspace();
      bool resize{false};
      bool replace{false};
      if (ds.type() == Type::Scalar && !shape().empty()) {
        LOG_ERROR("Constant dataset {} has scalar dataspace but new config "
                  "shape is not scalar.",
                  name());
        replace = true;
        // somehow replace the dataspace in the dataset?
      }
      if (ds.type() == Type::Simple &&
          Simple(ds).current_dimensions() != shape()) {
        LOG_ERROR("Constant dataset {} has different shape than new config.",
                  name());
        resize = true;
        auto max_size = Simple(ds).maximum_dimensions();
        auto max_count = std::accumulate(max_size.cbegin(), max_size.cend(), 1,
                                         std::multiplies<>());
        auto count = std::accumulate(shape().cbegin(), shape().cend(), 1,
                                     std::multiplies<>());
        if (count > max_count) {
          LOG_ERROR("Constant dataset {} too small maximum size {} < {}.",
                    name(), max_count, count);
          // somehow replace the dataspace
          replace = true;
        }
      }
      // resizing/reshaping non-extensible datasets is not possible in SWMR-mode
      if (!swmr_enabled && (resize || replace)) {
        auto dataspace = Simple(shape(), shape());
        auto datatype = constant->datatype();
        auto link = constant->link();
        auto attrs = constant->attributes;
        hdf5::node::remove(*constant);
        auto dataset = std::make_unique<hdf5::node::Dataset>(
            link.parent(), link.path(), datatype, dataspace);
        constant = std::move(dataset);
      }
    }
  }
  template <class T>
  void update_constant(ConstantDataset &constant,
                       const std::vector<T> &data) const {
    update_constant(constant);
    std::vector<T> var_data;
    constant->read(var_data);
    if (var_data.size() != data.size()) {
      LOG_ERROR("Constant dataset {} has different size than new data.",
                name());
    }
    bool same{true};
    if constexpr (std::is_integral_v<T>) {
      same = var_data == data;
    } else {
      for (size_t i = 0; i < data.size(); ++i) {
        auto diff = var_data[i] - data[i];
        auto total = var_data[i] + data[i];
        same &= std::abs(diff / total) < 1e-6;
      }
    }
    if (!same) {
      LOG_ERROR("Constant dataset {} has different data than new data.",
                name());
      constant->write(data);
    }
  }

private:
  template <class Dataset>
  void add_dataset_attributes(Dataset &dataset,
                              const std::string &axes_spec) const {
    // Write fixed-size attributes in case we don't have 'real' values (yet)
    if (has_unit())
      unit().insert_attribute(dataset, "units");
    if (has_label())
      label().insert_attribute(dataset, "long_name");
    if (has_source())
      source().insert_attribute(dataset, "source");
    //
    if (!axes_spec.empty()) {
      auto datatype = hdf5::datatype::String::fixed(axes_spec.size());
      auto attr = dataset->attributes.create("axes", datatype,
                                             hdf5::dataspace::Scalar());
      attr.write(axes_spec, datatype);
    }
  }

private:
  std::string name_;
  std::optional<StringConfig> unit_;
  std::optional<StringConfig> label_;
  std::optional<StringConfig> source_;
  std::optional<da00_dtype> dtype_;
  std::optional<std::vector<hsize_t>> shape_;
  std::optional<std::vector<std::string>> axes_;
  std::optional<nlohmann::json> data_;
};

} // namespace WriterModule::da00

template <> struct fmt::formatter<WriterModule::da00::VariableConfig> {
  template <class ParseContext> constexpr auto parse(ParseContext &ctx) {
    return ctx.begin();
  }
  template <class FormatContext>
  auto format(const WriterModule::da00::VariableConfig &c, FormatContext &ctx) {
    auto unit = c.has_unit() ? fmt::format(c.unit(), ctx) : "none";
    auto label = c.has_label() ? fmt::format(c.label(), ctx) : "none";
    auto source = c.has_source() ? fmt::format(c.source(), ctx) : "none";
    auto type = c.has_dtype() ? c.dtype() : da00_dtype::none;
    auto shape = c.has_shape() ? c.shape() : std::vector<hsize_t>{};
    auto dims = c.has_axes() ? c.axes() : std::vector<std::string>{};
    auto data = c.has_data() ? c.json_data() : nlohmann::json{};
    return format_to(ctx.out(),
                     "VariableConfig(name={}, unit={}, label={}, source={}, "
                     "type={}, shape={}, axes={}, data={})",
                     c.name(), unit, label, source, type, shape, dims, data);
  }
};
