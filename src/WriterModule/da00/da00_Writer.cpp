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

namespace WriterModule::da00 {

// Register the file writing part of this module.
static WriterModule::Registry::Registrar<da00_Writer>
Register_da00_Writer("da00", "da00");

/// \brief Parse config JSON structure.
///
/// The default is to use double as the element type.
void da00_Writer::config_post_processing() {
  auto configs = DatasetsField.getValue();
  for (auto &cfg : configs) {
    VariableConfig v;
    v = cfg.dump();
    if (v.name().empty()) {
      throw WriterException("Configuration Variable name is empty.");
    }
    VariableMap[v.name()] = v; // this is generally a bad idea.
  }
  VariableNames = VariablesField.getValue();
  ConstantNames = ConstantsField.getValue();
  // these checks fulfill our contract with the user.
  // move any empty strings to the end, return the iterator to the first empty
  // (or end)

  if (const auto itr = std::remove_if(VariableNames.begin(), VariableNames.end(),
                            [](const std::string &s) { return s.empty(); });
                            itr != VariableNames.end()) {
    LOG_ERROR("Empty variable name in configuration.");
    VariableNames.erase(itr, VariableNames.end());
  }
  if (const auto itr = std::remove_if(ConstantNames.begin(), ConstantNames.end(),
                            [](const std::string &s) { return s.empty(); });
                            itr != ConstantNames.end()) {
    LOG_ERROR("Empty variable name in configuration.");
    ConstantNames.erase(itr, ConstantNames.end());
  }
}

void da00_Writer::handle_first_message(da00_DataArray const * da00) {
  // deal with variable and constant datasets that were not (fully) configured:
  // Any variable that is not pre-configured is skipped
  // (unless if there are _no_ preconfigured variables)
  if (VariableMap.empty() && VariablePtrs.empty() && ConstantPtrs.empty()) {
    // *No* pre-configured variables, so we record everything from this message
    for (const auto ptr : *da00->data()) {
      auto var = VariableConfig(ptr->name()->str());
      // check for unit, label, data_type, shape, dims, (data)
      if (ptr->unit() != nullptr) var.unit(ptr->unit()->str());
      if (ptr->label() != nullptr) var.label(ptr->label()->str());
      if (ptr->shape() != nullptr) {
        VariableConfig::dim_t const shape{ptr->shape()->begin(), ptr->shape()->end()};
        var.shape(shape);
      } else {
        // find the size of the data, use that as the shape
        auto size = ptr->data()->size();
        var.shape({size});
      }
      var.type(ptr->data_type());
      std::vector<std::string> dims;
      dims.reserve(var.shape().size());
      if (ptr->dims() != nullptr) {
        for (const auto dim : *ptr->dims()) {
          dims.push_back(dim->str());
        }
      } else {
        for (size_t i=0; i<var.shape().size(); ++i) {
          dims.push_back(fmt::format("dim{}", i));
        }
      }
      var.dims(dims);
      if (ConstantNames.empty() && VariableNames.empty()) {
        // No specified variables or constants, so we record everything
        VariableMap[var.name()] = var;
        VariablePtrs[var.name()] = var.insert_variable_dataset(Parent, ChunkSize);
      } else if (std::find(ConstantNames.begin(), ConstantNames.end(), var.name()) != ConstantNames.end()) {
        // This is a specified constant, so we record it as such
        VariableMap[var.name()] = var;
        ConstantPtrs[var.name()] = var.insert_constant_dataset(Parent);
      } else if (std::find(VariableNames.begin(), VariableNames.end(), var.name()) != VariableNames.end()){
        // This is a specified variable, so we record it as such
        VariableMap[var.name()] = var;
        VariablePtrs[var.name()] = var.insert_variable_dataset(Parent, ChunkSize);
      } else {
        LOG_WARN("Variable {} is not configured. Buffered data is ignored", var.name());
      }
    }
  } else {
    // TODO Consider skipping all of this, and just using the configured values?
    // Check if each message-variable is a variable or constant,
    // update their parameters if neccessary (and replace their dataset...)
    for (const auto ptr: *da00->data()) {
      auto name{ptr->name()->str()};
      auto pair = VariableMap.find(name);
      if (pair == VariableMap.end()) {
        LOG_WARN("Variable {} is not configured. Buffered data is ignored", name);
        continue;
      }
      auto & v = pair->second;
      // check for unit, label, data_type, shape, dims, (data)
      bool updated{false};
      if (const auto has = v.has_unit(); !has && ptr->unit() != nullptr) {
        v.unit(ptr->unit()->str());
        updated = true;
      } else if (has && ptr->unit() != nullptr && v.unit() != ptr->unit()->str()) {
        LOG_WARN("Unit for variable {} is not consistent. Using configured value.", name);
      }
      if (const auto has = v.has_label(); !has && ptr->label() != nullptr) {
        v.label(ptr->label()->str());
        updated = true;
      } else if (has && ptr->label() != nullptr && v.label() != ptr->label()->str()) {
        LOG_WARN("Label for variable {} is not consistent. Using configured value.", name);
      }
      if (const auto has = v.has_type(); !has) {
        v.type(ptr->data_type());
        updated = true;
      } else if (v.type() != dtype_to_da00_type(ptr->data_type())) {
        LOG_WARN("Data type for variable {} is not consistent. Using configured value.", name);
      }
      if (const auto has = v.has_shape(); !has && ptr->shape() != nullptr) {
        VariableConfig::dim_t const shape{ptr->shape()->begin(), ptr->shape()->end()};
        v.shape(shape);
        updated = true;
      } else if (has && ptr->shape() != nullptr) {
        if (VariableConfig::dim_t const shape{ptr->shape()->begin(), ptr->shape()->end()}; v.shape() != shape) {
          LOG_WARN("Shape for variable {} is not consistent. Using configured value.", name);
        }
      }
      if (const auto has = v.has_dims(); !has && ptr->dims() != nullptr) {
        std::vector<std::string> dims;
        dims.reserve(v.shape().size());
        for (const auto dim : *ptr->dims()) {
          dims.push_back(dim->str());
        }
        v.dims(dims);
        updated = true;
      } else if (has && ptr->dims() != nullptr) {
        size_t i{0};
        const auto & dims = v.dims();
        if (ptr->dims()->size() != dims.size()) {
          LOG_WARN("Number of dimensions for variable {} is not consistent. Using configured value.", name);
        }
        if (ptr->dims()->size() <= dims.size()) {
          for (const auto dim : *ptr->dims()) {
            if (dims[i] != dim->str()) {
              LOG_WARN("Dimension {} for variable {} is not consistent. Using configured value.", i, name);
            }
            ++i;
          }
        } else {
          LOG_ERROR("Buffer has more dims than configuration for {}", name);
        }
      }
      if (auto f = VariablePtrs.find(name); f != VariablePtrs.end()) {
        if (updated) {
          Parent.remove(name);
          VariablePtrs[name] = v.insert_variable_dataset(Parent, ChunkSize);
        }
      } else if (auto g = ConstantPtrs.find(name); g != ConstantPtrs.end()) {
        // check for data consistency
        bool needs_data{false};
        if (const auto has = v.has_data(); !has && ptr->data() != nullptr) {
          needs_data = true;
        } else if (has && ptr->data() != nullptr) {
          if (!v.compare_data(std::vector(ptr->data()->begin(), ptr->data()->end()))){
            LOG_WARN("Data for constant {} is not consistent. Using configured value.", name);
          }
        }
        if (updated) {
          Parent.remove(name);
          ConstantPtrs[name] = v.insert_constant_dataset(Parent);
        }
        if (needs_data) {
          v.write_constant_dataset(ConstantPtrs[name], ptr->data()->Data(), ptr->data()->size());
        }
      }
    }
  }
}

InitResult da00_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  const auto chunk_size = ChunkSize.operator hdf5::Dimensions().at(0);
  using NeXusDataset::Mode;
  try {
    for (const auto & name: VariableNames){
      if (auto f = VariableMap.find(name); f != VariableMap.end()) {
        auto unused = f->second.insert_variable_dataset(HDFGroup, ChunkSize);
      } else {
        LOG_WARN("Variable {} is not configured. Will insert dataset at first message", name);
      }
    }
    for (const auto & name: ConstantNames){
      if (auto f = VariableMap.find(name); f != VariableMap.end()) {
        auto unused = f->second.insert_constant_dataset(HDFGroup);
      } else {
        LOG_WARN("Constant {} is not configured. Will insert dataset at first message", name);
      }
    }
    NeXusDataset::Time(HDFGroup, Mode::Create, chunk_size); // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueIndex(HDFGroup, Mode::Create, chunk_size); // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueTimestampZero(HDFGroup, Mode::Create, chunk_size); // NOLINT(bugprone-unused-raii)
  } catch (std::exception &E) {
    LOG_ERROR(
        R"(Unable to initialise areaDetector data tree in HDF file with error message: "{}")",
        E.what());
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

InitResult da00_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    for (const auto & name: VariableNames){
      if (auto f = VariableMap.find(name); f != VariableMap.end()){
        VariablePtrs[name] = f->second.reopen_variable_dataset(HDFGroup);
      } else {
        LOG_WARN("Variable {} is not configured. Will insert dataset at first message", name);
      }
    }
    for (const auto & name: ConstantNames){
      if (auto f = VariableMap.find(name); f != VariableMap.end()){
        ConstantPtrs[name] = f->second.reopen_constant_dataset(HDFGroup);
      } else {
        LOG_WARN("Constant {} is not configured. Will insert dataset at first message", name);
      }
    }
    CueIndex = NeXusDataset::CueIndex(HDFGroup, NeXusDataset::Mode::Open);
    CueTimestampZero = NeXusDataset::CueTimestampZero(HDFGroup, NeXusDataset::Mode::Open);
    Timestamp = NeXusDataset::Time(HDFGroup, NeXusDataset::Mode::Open);
    Parent = HDFGroup; // stash away a reference to the group... hopefully this is valid.
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
  // const auto CurrentTimestamp = da00->timestamp();
  // go through the buffered Variables and write non-constants:
  for (const auto ptr: *da00_obj->data()) {
    auto name = ptr->name()->str();
    if (auto f = VariablePtrs.find(name); f != VariablePtrs.end()) {
      if (!f->second->is_valid()) {
        LOG_ERROR("Variable {} dataset pointer is not valid. Buffered data is ignored", name);
      }
      VariableMap[name].variable_append(f->second, ptr);
    }
  }
  Timestamp.appendElement(da00_obj->timestamp());
  if (++CueCounter == CueInterval) {
    CueIndex.appendElement(Timestamp.dataspace().size() - 1);
    CueTimestampZero.appendElement(da00_obj->timestamp());
    CueCounter = 0;
  }
}

} // namespace WriterModule::da00

