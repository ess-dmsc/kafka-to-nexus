#pragma once

#include <nlohmann/json.hpp>
#include <h5cpp/hdf5.hpp>
#include <optional>
#include <string>
#include "logger.h"
#include "da00_Type.h"

namespace WriterModule::da00{

class AttributeConfig {
  using dtype_t = da00_dtype;
public:
  AttributeConfig() = default;
  explicit AttributeConfig(const nlohmann::json &config){
    if (!config.contains("name")){
      throw std::runtime_error("AttributeConfig: no name given");
    }
    _name = config["name"];
    if (config.contains("description")) {
      _description = config["description"];
    }
    if (config.contains("source")) {
      _source = config["source"];
    }
    if (config.contains("data_type")){
      _type = string_to_da00_dtype(config["data_type"]);
    } else {
      auto x = config["data"];
      if (x.is_array()) x = x[0];
      if (x.is_number_unsigned()){
        _type = dtype_t::uint64;
      } else if (x.is_number_integer()){
        _type = dtype_t::int64;
      } else if (x.is_number_float()){
        _type = dtype_t::float64;
      } else if (x.is_string()){
        _type = dtype_t::c_string;
      } else {
        throw std::runtime_error("AttributeConfig: could not determine data type of " + config["data"].dump());
      }
    }
    _data = config["data"];
  }
  AttributeConfig & operator=(const std::string & config){
    auto other = AttributeConfig(nlohmann::json::parse(config));
    *this = other;
    return *this;
  }
  AttributeConfig & operator=(const AttributeConfig & other) = default;
  AttributeConfig & operator=(AttributeConfig && other) = default;
  AttributeConfig(const AttributeConfig & other) = default;
  AttributeConfig(AttributeConfig && other) = default;
  ~AttributeConfig() = default;

  [[nodiscard]] bool has_description() const { return _description.has_value(); }
  [[nodiscard]] bool has_source() const { return _source.has_value(); }
  [[nodiscard]] da00_dtype type() const { return _type; }
  [[nodiscard]] const std::string & name() const { return _name; }
  [[nodiscard]] const std::string & description() const { return _description.value(); }
  [[nodiscard]] const std::string & source() const { return _source.value(); }
  [[nodiscard]] const nlohmann::json & data() const { return _data; }
  void data(nlohmann::json value) { _data = std::move(value); }

private:
  std::string _name;
  std::optional<std::string> _description;
  std::optional<std::string> _source;
  da00_dtype _type;
  nlohmann::json _data;

  template<class T>
  void _add_to_hdf5(hdf5::node::Node & node) const {
    auto the_data = data().get<T>();
    node.attributes.create_from(name(), the_data);
  }

  void add_scalar_to_hdf5(hdf5::node::Node & node) const {
    std::map<da00_dtype, std::function<void()>> type_map{
      {dtype_t::int8, [&](){ _add_to_hdf5<std::int8_t>(node); }},
      {dtype_t::uint8, [&](){ _add_to_hdf5<std::uint8_t>(node); }},
      {dtype_t::int16, [&](){ _add_to_hdf5<std::int16_t>(node); }},
      {dtype_t::uint16, [&](){ _add_to_hdf5<std::uint16_t>(node); }},
      {dtype_t::int32, [&](){ _add_to_hdf5<std::int32_t>(node); }},
      {dtype_t::uint32, [&](){ _add_to_hdf5<std::uint32_t>(node); }},
      {dtype_t::int64, [&](){ _add_to_hdf5<std::int64_t>(node); }},
      {dtype_t::uint64, [&](){ _add_to_hdf5<std::uint64_t>(node); }},
      {dtype_t::float32, [&](){ _add_to_hdf5<std::float_t>(node); }},
      {dtype_t::float64, [&](){ _add_to_hdf5<std::double_t>(node); }},
      {dtype_t::c_string, [&](){ _add_to_hdf5<std::string>(node); }},
    };
    type_map[type()]();
  }

  void add_array_to_hdf5(hdf5::node::Node & node) const {
    std::map<da00_dtype, std::function<void()>> type_map{
      {dtype_t::int8, [&](){ _add_to_hdf5<std::vector<std::int8_t>>(node); }},
      {dtype_t::uint8, [&](){ _add_to_hdf5<std::vector<std::uint8_t>>(node); }},
      {dtype_t::int16, [&](){ _add_to_hdf5<std::vector<std::int16_t>>(node); }},
      {dtype_t::uint16, [&](){ _add_to_hdf5<std::vector<std::uint16_t>>(node); }},
      {dtype_t::int32, [&](){ _add_to_hdf5<std::vector<std::int32_t>>(node); }},
      {dtype_t::uint32, [&](){ _add_to_hdf5<std::vector<std::uint32_t>>(node); }},
      {dtype_t::int64, [&](){ _add_to_hdf5<std::vector<std::int64_t>>(node); }},
      {dtype_t::uint64, [&](){ _add_to_hdf5<std::vector<std::uint64_t>>(node); }},
      {dtype_t::float32, [&](){ _add_to_hdf5<std::vector<std::float_t>>(node); }},
      {dtype_t::float64, [&](){ _add_to_hdf5<std::vector<std::double_t>>(node); }},
      {dtype_t::c_string, [&](){ _add_to_hdf5<std::vector<std::string>>(node); }},
    };
    type_map[type()]();
  }

public:
  void add_to_hdf5(hdf5::node::Node & node) const {
    if (data().is_array()){
      add_array_to_hdf5(node);
    } else if (data().is_number() || data().is_string()){
      add_scalar_to_hdf5(node);
    } else {
      throw std::runtime_error("Attribute value is neither scalar nor array");
    }
  }
};
}
