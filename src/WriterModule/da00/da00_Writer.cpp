// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2021 European Spallation Source ERIC */

/// \file
/// \brief Implement classes required to implement the ADC file writing module.

#include "helper.h"

#include "HDFOperations.h"
#include "WriterRegistrar.h"
#include "da00_Writer.h"
#include <da00_dataarray_generated.h>
#include <f142_logdata_generated.h>
#include <trompeloeil.hpp>

#include "da00_Variable.h"
#include "da00_Attribute.h"

namespace WriterModule::da00 {

// Register the file writing part of this module.
static WriterModule::Registry::Registrar<da00_Writer> Register_da00_Writer("da00", "da00");

static void fill_config_map(std::map<std::string, VariableConfig>& map, const std::vector<nlohmann::json> & configs) {
  for (const auto & config: configs){
    VariableConfig v;
    v = config.dump();
    if (v.name().empty()) {
      throw WriterException("Configuration Variable name is empty.");
    }
    map[v.name()] = v;
  }
}

/// \brief Parse config JSON structure.
///
/// The default is to use double as the element type.
void da00_Writer::config_post_processing() {
  try {
    fill_config_map(VariableConfigMap, VariablesField.getValue());
    fill_config_map(ConstantConfigMap, ConstantsField.getValue());
  } catch (std::exception &E) {
    LOG_ERROR("Failed to parse configuration with error message: {}", E.what());
    throw;
  }
}


template <typename... Args>
static void warn_if(bool Condition, const std::string & fmt, const Args &... args) {
  std::stringstream msg;
  msg << fmt << " Using configured value.\n";
  if (Condition) LOG_WARN(msg.str(), args...);
}

void da00_Writer::handle_first_message(da00_DataArray const * da00) {
  if (!VariableConfigMap.empty()){
    for (const auto ptr: *da00->data()){
      auto fb = VariableConfig(ptr);
      auto p = VariableConfigMap.find(fb.name());
      if (p == VariableConfigMap.end()) continue;
      auto & v = p->second;
      auto inconsistent_changed = v.update_from(fb, false);
      if (inconsistent_changed.first)
        LOG_WARN("Variable {} is configured with inconsistent data", fb.name());
      if (inconsistent_changed.second) {
        LOG_DEBUG("Variable {} changed, find it's dataset and update it", fb.name());
        if (auto f = VariablePtrs.find(fb.name()); f != VariablePtrs.end()) {
          v.update_variable(f->second);
        } else {
          LOG_ERROR("Unable to find dataset to update for Variable {}", fb.name());
          for (const auto & [name, dptr] : VariablePtrs) {
            LOG_ERROR("Found dataset for Variable {}", name);
          }
        }
      }
    }
  }
  if (!ConstantConfigMap.empty()) {
    for (const auto ptr: *da00->data()){
      auto fb = VariableConfig(ptr);
      auto p = ConstantConfigMap.find(fb.name());
      if (p == ConstantConfigMap.end()) continue;
      auto & v = p->second;
      auto inconsistent_changed = v.update_from(fb, false);
      // check for data consistency
      bool needs_data{false};
      if (const auto has = v.has_data(); !has && ptr->data() != nullptr) {
        needs_data = true;
      } else if (has && ptr->data() != nullptr) {
        warn_if(!v.compare_data(std::vector(ptr->data()->begin(), ptr->data()->end())),
                "Data for constant {} is not consistent.", fb.name());
      }
      if (inconsistent_changed.second || needs_data){
        LOG_INFO("Constant {} changed or needs data written into it, find it's dataset and update it", fb.name());
        if (auto f= ConstantPtrs.find(fb.name()); f != ConstantPtrs.end()){
          if (inconsistent_changed.second)
            v.update_constant(f->second);
          if (needs_data)
            v.write_constant_dataset(f->second, ptr->data()->Data(), ptr->data()->size());
        } else {
          LOG_ERROR("Unable to find dataset to update for Constant {}", fb.name());
          for (const auto & [name, dptr] : ConstantPtrs) {
            LOG_ERROR("Found dataset for Constant {}", name);
          }
        }
      }
    }
  }
}

void da00_Writer::handle_group_attributes(hdf5::node::Group &HDFGroup) const {
  /*
   * The config should specify attributes, but might not include the
   * leading 'time' axis on a 'signal' dataset which is also a Variable.
   * If the 'signal' is a Variable, then we should add the 'time' axis
   * to the 'axes' attribute.
   * If the 'axes' attribute is missing, we can try to create it from the
   * Variable's `axes` attribute.
   */
  // in case we need the first variable name (don't use the map to preserve order)
  auto variable_configs = VariablesField.getValue();
  std::string first_variable_name;
  if (!variable_configs.empty()) {
    VariableConfig vc;
    vc = variable_configs.front().dump();
    first_variable_name = vc.name();
  }

  std::vector<AttributeConfig> attrs;
  auto attrs_json = AttributesField.getValue();
  attrs.reserve(attrs_json.size());
  auto signal_is_variable{false}, signal_is_present{false};
  std::string signal_name;
  for (const auto &js : attrs_json) {
    attrs.emplace_back(js);
    if (attrs.back().name() == "signal") {
      signal_is_present = true;
      auto value = attrs.back().data();
      if (value.is_string()) {
        signal_name = value.get<std::string>();
        if (VariableConfigMap.find(signal_name) != VariableConfigMap.end()) {
          signal_is_variable = true;
        }
      }
    }
  }
  if (!signal_is_present && !first_variable_name.empty()) {
    signal_name = first_variable_name;
    auto signal_json = nlohmann::json::object();
    signal_json["name"] = "signal";
    signal_json["data"] = signal_name;
    attrs.emplace_back(signal_json);
    signal_is_variable = true;
  }
  // no "axes" specified, so add it
  if (attrs.end() == std::find_if(attrs.begin(), attrs.end(),
                                  [](const AttributeConfig & attr){ return attr.name() == "axes"; })) {
    if (auto f= VariableConfigMap.find(signal_name); f != VariableConfigMap.end()) {
      if (!f->second.has_axes()){
        LOG_ERROR("Configuration for Variable {} has no axes!", signal_name);
      } else {
        auto axes = f->second.axes();
        if (!axes.empty()) {
          auto axes_json = nlohmann::json::object();
          axes_json["name"] = "axes";
          axes_json["data"] = axes;
          attrs.emplace_back(axes_json);
        }
      }
    }
  }

  // find the "axes" again, in case pointers were invalidated
  // if the signal is a Variable, then add 'time' to the 'axes' attribute
  if (auto axes_at = std::find_if(attrs.begin(), attrs.end(), [](const AttributeConfig & attr){ return attr.name() == "axes"; });
      signal_is_variable && axes_at != attrs.end()) {
    // find 'axes' if is an attribute, add 'time' before the other axes names
    auto value = axes_at->data();
    if (value.is_array()) {
      auto axes = value.get<std::vector<std::string>>();
      axes.insert(axes.begin(), "time");
      auto new_value = nlohmann::json::array();
      new_value = axes;
      axes_at->data(new_value);
    }
  }
  // all attributes _should_ be right (or as right as we can get them)
  // So write them all into the group's attributes field
  for (const auto & attr: attrs){
    try {
      attr.add_to_hdf5(HDFGroup);
    } catch (std::exception &E) {
      LOG_ERROR("Failed to add attribute `{}` to HDF file with error message: {}", attr.name(), E.what());
    }
  }
}


InitResult da00_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  const auto chunk_size = ChunkSize.operator hdf5::Dimensions().at(0);
  using NeXusDataset::Mode;
  handle_group_attributes(HDFGroup);
  try {
    /* Instantiate the children datasets */
    for (const auto & [name, config] : VariableConfigMap){
      if (config.has_dtype() && config.has_shape()) {
        auto unused = config.insert_variable_dataset(HDFGroup, ChunkSize);
      } else {
        if (!config.has_dtype())
          LOG_ERROR("Variable {} configuration lacks data_type. Can not insert dataset", name);
        if (!config.has_shape())
          LOG_ERROR("Variable {} configuration lacks shape. Can not insert dataset", name);
      }
    }
    for (const auto & [name, config]: ConstantConfigMap) {
      if ((config.has_dtype() && config.has_shape()) || config.has_data()) {
        auto unused = config.insert_constant_dataset(HDFGroup);
      } else {
        if (!config.has_data())
          LOG_ERROR("Constant {} configuration lacks data. Can not insert dataset", name);
        if (!config.has_dtype())
          LOG_ERROR("Constant {} configuration lacks data_type. Can not insert dataset", name);
        if (!config.has_shape())
          LOG_ERROR("Constant {} configuration lacks shape. Can not insert dataset", name);
      }
    }
    NeXusDataset::Time(HDFGroup, Mode::Create, chunk_size); // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueIndex(HDFGroup, Mode::Create, chunk_size); // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueTimestampZero(HDFGroup, Mode::Create, chunk_size); // NOLINT(bugprone-unused-raii)
  } catch (std::exception &E) {
    LOG_ERROR(
        R"(Unable to initialise DataArray data tree in HDF file with error message: "{}")",
        E.what());
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

InitResult da00_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    for (const auto & [name, config] : VariableConfigMap){
      if (config.has_dtype() && config.has_shape()) {
        VariablePtrs[name] = config.reopen_variable_dataset(HDFGroup);
      } else {
        if (!config.has_dtype())
          LOG_ERROR("Variable {} configuration lacks data_type. Can not reopen dataset", name);
        if (!config.has_shape())
          LOG_ERROR("Variable {} configuration lacks shape. Can not reopen dataset", name);
      }
    }
    for (const auto & [name, config]: ConstantConfigMap) {
      if ((config.has_dtype() && config.has_shape()) || config.has_data()) {
        ConstantPtrs[name] = config.reopen_constant_dataset(HDFGroup);
      } else {
        if (!config.has_data())
          LOG_ERROR("Constant {} configuration lacks data. Can not reopen dataset", name);
        if (!config.has_dtype())
          LOG_ERROR("Constant {} configuration lacks data_type. Can not reopen dataset", name);
        if (!config.has_shape())
          LOG_ERROR("Constant {} configuration lacks shape. Can not reopen dataset", name);
      }
    }
    CueIndex = cueindex_t(HDFGroup, NeXusDataset::Mode::Open);
    CueTimestampZero = cuezero_t(HDFGroup, NeXusDataset::Mode::Open);
    Timestamp = timestamp_t(HDFGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    LOG_ERROR(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

void da00_Writer::writeImpl(const FileWriter::FlatbufferMessage &Message) {
  const auto da00_obj = Getda00_DataArray(Message.data());
  if (isFirstMessage) {
    handle_first_message(da00_obj);
    isFirstMessage = false;
  }
  // go through the buffered data and write non-constants:
  std::vector<std::string> variable_order;
  variable_order.reserve(VariablePtrs.size());
  for (const auto ptr: *da00_obj->data()) {
    auto name = ptr->name()->str();
    if (auto f = VariablePtrs.find(name); f != VariablePtrs.end()) {
      if (!f->second->is_valid()) {
        LOG_ERROR("Variable {} dataset pointer is not valid. Buffered data is ignored", name);
      }
      VariableConfigMap[name].variable_append(f->second, ptr);
      variable_order.push_back(name);
    } else {
      LOG_DEBUG("Buffer Variable {} is not a configured dataset. Buffered data is ignored", name);
    }
  }
  if (variable_order.size() != VariablePtrs.size()) {
    std::stringstream message;
    if (VariablePtrs.size() - variable_order.size() > 1) {
      message << "Buffered data is missing variables ";
    } else {
      message << "Buffered data is missing variable ";
    }
    for (auto & [name, ptr]: VariablePtrs) {
      if (std::find(variable_order.begin(), variable_order.end(), name) == variable_order.end()) {
        VariableConfigMap[name].variable_append_missing(ptr);
        message << name << ", ";
      }
    }
    auto str = message.str();
    str.erase(str.size() - 2);
    LOG_ERROR(str);
  }
  Timestamp.appendElement(da00_obj->timestamp());
  if (++CueCounter == CueInterval) {
    CueIndex.appendElement(Timestamp.dataspace().size() - 1);
    CueTimestampZero.appendElement(da00_obj->timestamp());
    CueCounter = 0;
  }
}

} // namespace WriterModule::da00

