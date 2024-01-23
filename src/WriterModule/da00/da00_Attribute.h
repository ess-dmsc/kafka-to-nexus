#pragma once

#include <nlohmann/json.hpp>
#include <h5cpp/hdf5.hpp>
#include <optional>
#include <string>
#include "logger.h"
#include "da00_Type.h"

namespace WriterModule::da00{

class AttributeConfig {
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
      _type = string_to_da00_type(config["data_type"]);
    } else {
      auto x = config["value"];
      if (x.is_array()) x = x[0];
      if (x.is_number_unsigned()){
        _type = da00_type::uint64;
      } else if (x.is_number_integer()){
        _type = da00_type::int64;
      } else if (x.is_number_float()){
        _type = da00_type::float64;
      } else if (x.is_string()){
        _type = da00_type::c_string;
      } else {
        throw std::runtime_error("AttributeConfig: could not determine data type of " + config["value"].dump());
      }
    }
    _value = config["value"];
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
  [[nodiscard]] da00_type type() const { return _type; }
  [[nodiscard]] const std::string & name() const { return _name; }
  [[nodiscard]] const std::string & description() const { return _description.value(); }
  [[nodiscard]] const std::string & source() const { return _source.value(); }
  [[nodiscard]] const nlohmann::json & value() const { return _value; }
  void value(nlohmann::json value) { _value = std::move(value); }

private:
  std::string _name;
  std::optional<std::string> _description;
  std::optional<std::string> _source;
  da00_type _type;
  nlohmann::json _value;

  template<class T>
  void _add_to_hdf5(hdf5::node::Node & node) const {
    auto data = value().get<T>();
    node.attributes.create_from(name(), data);
  }

  void add_scalar_to_hdf5(hdf5::node::Node & node) const {
    std::map<da00_type, std::function<void()>> type_map{
      {da00_type::int8, [&](){ _add_to_hdf5<std::int8_t>(node); }},
      {da00_type::uint8, [&](){ _add_to_hdf5<std::uint8_t>(node); }},
      {da00_type::int16, [&](){ _add_to_hdf5<std::int16_t>(node); }},
      {da00_type::uint16, [&](){ _add_to_hdf5<std::uint16_t>(node); }},
      {da00_type::int32, [&](){ _add_to_hdf5<std::int32_t>(node); }},
      {da00_type::uint32, [&](){ _add_to_hdf5<std::uint32_t>(node); }},
      {da00_type::int64, [&](){ _add_to_hdf5<std::int64_t>(node); }},
      {da00_type::uint64, [&](){ _add_to_hdf5<std::uint64_t>(node); }},
      {da00_type::float32, [&](){ _add_to_hdf5<std::float_t>(node); }},
      {da00_type::float64, [&](){ _add_to_hdf5<std::double_t>(node); }},
      {da00_type::c_string, [&](){ _add_to_hdf5<std::string>(node); }},
    };
    type_map[type()]();
  }

  void add_array_to_hdf5(hdf5::node::Node & node) const {
    std::map<da00_type, std::function<void()>> type_map{
      {da00_type::int8, [&](){ _add_to_hdf5<std::vector<std::int8_t>>(node); }},
      {da00_type::uint8, [&](){ _add_to_hdf5<std::vector<std::uint8_t>>(node); }},
      {da00_type::int16, [&](){ _add_to_hdf5<std::vector<std::int16_t>>(node); }},
      {da00_type::uint16, [&](){ _add_to_hdf5<std::vector<std::uint16_t>>(node); }},
      {da00_type::int32, [&](){ _add_to_hdf5<std::vector<std::int32_t>>(node); }},
      {da00_type::uint32, [&](){ _add_to_hdf5<std::vector<std::uint32_t>>(node); }},
      {da00_type::int64, [&](){ _add_to_hdf5<std::vector<std::int64_t>>(node); }},
      {da00_type::uint64, [&](){ _add_to_hdf5<std::vector<std::uint64_t>>(node); }},
      {da00_type::float32, [&](){ _add_to_hdf5<std::vector<std::float_t>>(node); }},
      {da00_type::float64, [&](){ _add_to_hdf5<std::vector<std::double_t>>(node); }},
      {da00_type::c_string, [&](){ _add_to_hdf5<std::vector<std::string>>(node); }},
    };
    type_map[type()]();
  }

public:
  void add_to_hdf5(hdf5::node::Node & node) const {
    if (value().is_array()){
      add_array_to_hdf5(node);
    } else if (value().is_number() || value().is_string()){
      add_scalar_to_hdf5(node);
    } else {
      throw std::runtime_error("Attribute value is neither scalar nor array");
    }
  }
};
}
