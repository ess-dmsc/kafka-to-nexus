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

template <typename... Args>
static void warn_if(bool Condition, const std::string & fmt, const Args &... args) {
  std::stringstream msg;
  msg << fmt << " Using configured value.\n";
  if (Condition) LOG_WARN(msg.str(), args...);
}
void da00_Writer::handle_first_message(da00_DataArray const * da00) {
  auto dynamic = DynamicDatasets.getValue();
  auto make_dataset = [&](const VariableConfig & vc){
    if (!dynamic) {
      LOG_ERROR("Variable {} is not configured and writer is static. Buffered data is ignored", vc.name());
      return;
      }
    auto name = vc.name();
    bool is_variable{true};
    if (VariableNames.empty() && ConstantNames.empty()) {
        // No specified variables or constants, so we record everything
      VariablePtrs[name] = vc.insert_variable_dataset(Parent, ChunkSize);
    } else if (std::find(ConstantNames.begin(), ConstantNames.end(), name) != ConstantNames.end()) {
        // This is a specified constant, so we record it as such
      ConstantPtrs[name] = vc.insert_constant_dataset(Parent);
      is_variable = false;
    } else if (std::find(VariableNames.begin(), VariableNames.end(), name) != VariableNames.end()){
        // This is a specified variable, so we record it as such
      VariablePtrs[name] = vc.insert_variable_dataset(Parent, ChunkSize);
      } else {
      LOG_WARN("Variable {} is not configured. Buffered data is ignored", name);
      return;
      }
    VariableMap[name] = vc;
    if (is_variable){
      VariablePtrs[name]->refresh();
  } else {
      ConstantPtrs[name]->refresh();
    }
  };

  // deal with variable and constant datasets that were not (fully) configured:
  // Any variable that is not pre-configured is skipped
  // (unless if there are _no_ pre-configured variables)
  if (VariableMap.empty() && VariablePtrs.empty() && ConstantPtrs.empty()) {
    if (dynamic) {
      // *No* pre-configured variables, so we record everything
    for (const auto ptr: *da00->data()) {
        LOG_DEBUG("Handling first case variable {}", ptr->name()->str());
        make_dataset(VariableConfig(ptr));
      }
    } else {
      LOG_ERROR("No configuration for static writer. Buffered da00 data is ignored");
          }
        } else {
    // Check if each message-variable is a variable or constant,
    // update their parameters (and create their datasets) if necessary
    for (const auto ptr: *da00->data()) {
      LOG_DEBUG("Handle second case variable {}", ptr->name()->str());
      auto fb = VariableConfig(ptr);
      auto p = VariableMap.find(fb.name());
      if (p == VariableMap.end()) continue;
      auto & v = p->second;
      auto inconsistent_changed = v.update_from(fb, true);
      // if no dataset has been made for this Variable yet, we must make it now
      if (auto f = VariablePtrs.find(fb.name()); f == VariablePtrs.end()) {
        if (auto g = ConstantPtrs.find(fb.name()); g == ConstantPtrs.end()) {
          make_dataset(v);
          inconsistent_changed.second = false; // no need to update the dataset
        }
      }
      if (auto f = VariablePtrs.find(fb.name()); f != VariablePtrs.end()) {
        if (inconsistent_changed.second) {
          v.update_variable(f->second);
        }
      } else if (auto g = ConstantPtrs.find(fb.name()); g != ConstantPtrs.end()) {
        // check for data consistency
        bool needs_data{false};
        if (const auto has = v.has_data(); !has && ptr->data() != nullptr) {
          needs_data = true;
        } else if (has && ptr->data() != nullptr) {
          warn_if(!v.compare_data(std::vector(ptr->data()->begin(), ptr->data()->end())),
            "Data for constant {} is not consistent.", fb.name());
          }
        if (inconsistent_changed.second) {
          v.update_constant(g->second);
        }
        if (needs_data) {
          v.write_constant_dataset(g->second, ptr->data()->Data(), ptr->data()->size());
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
      if (auto f = VariableMap.find(name); f != VariableMap.end() && f->second.is_buildable()) {
        auto unused = f->second.insert_variable_dataset(HDFGroup, ChunkSize);
      } else if (f != VariableMap.end()) {
        LOG_WARN("Variable {} was configured without shape. Will insert dataset at first message if dynamic", name);
      } else {
        LOG_WARN("Variable {} is not configured. Will insert dataset at first message if dynamic", name);
      }
    }
    for (const auto & name: ConstantNames){
      if (auto f = VariableMap.find(name); f != VariableMap.end() && f->second.is_buildable()) {
        auto unused = f->second.insert_constant_dataset(HDFGroup);
      } else if (f != VariableMap.end()) {
        LOG_WARN("Constant {} was configured without shape. Will insert dataset at first message if dynamic", name);
      } else {
        LOG_WARN("Constant {} is not configured. Will insert dataset at first message if dynamic", name);
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
      if (auto f = VariableMap.find(name); f != VariableMap.end() && f->second.is_buildable()){
        VariablePtrs[name] = f->second.reopen_variable_dataset(HDFGroup);
      } else {
        LOG_WARN("Variable {} dataset is not configured. Dataset created at first message if dynamic", name);
      }
    }
    for (const auto & name: ConstantNames){
      if (auto f = VariableMap.find(name); f != VariableMap.end() && f->second.is_buildable()){
        ConstantPtrs[name] = f->second.reopen_constant_dataset(HDFGroup);
      } else {
        LOG_WARN("Constant {} dataset is not configured. Dataset created at first message if dynamic", name);
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

