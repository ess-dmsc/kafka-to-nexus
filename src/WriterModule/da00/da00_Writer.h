// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2024 European Spallation Source ERIC */

/// \file
/// \brief Main Writer class for the da00 flatbuffer DataArray schema.

#pragma once

#include "FlatbufferMessage.h"
#include "HDFFile.h"
#include "Msg.h"
#include "NeXusDataset/NeXusDataset.h"
#include "WriterModuleBase.h"

#include "da00_Type.h"
#include "da00_Variable.h"

namespace WriterModule::da00 {
/// See parent class for documentation.
class da00_Writer : public WriterModule::Base {
public:
  da00_Writer() : WriterModule::Base("da00", false, "NXdata") {}
  ~da00_Writer() override = default;

  void config_post_processing() override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  bool writeImpl(FileWriter::FlatbufferMessage const &Message,
                 bool is_buffered_message) override;

  NeXusDataset::Time Timestamp;
  NeXusDataset::CueIndex CueIndex;
  NeXusDataset::CueTimestampZero CueTimestampZero;

protected:
  // register config keys, parsed and filled-in by parent class
  JsonConfig::Field<uint64_t> CueInterval{
      this, "cue_interval", (std::numeric_limits<uint64_t>::max)()};
  JsonConfig::Field<hdf5::Dimensions> ChunkSize{this, "chunk_size", {1 << 20}};
  JsonConfig::Field<std::vector<nlohmann::json>> VariablesField{
      this, "variables", {}};
  JsonConfig::Field<std::vector<nlohmann::json>> ConstantsField{
      this, "constants", {}};
  JsonConfig::Field<std::vector<nlohmann::json>> AttributesField{
      this, "attributes", {}};
  uint64_t CueCounter{0};
  uint64_t NrOfWrites{0};
  bool isFirstMessage{true};

private:
  void handle_first_message(da00_DataArray const *da00);
  void handle_group_attributes(hdf5::node::Group &HDFGroup) const;
  // specifications for variable and constant datasets
  std::map<std::string, VariableConfig> VariableConfigMap;
  std::map<std::string, VariableConfig> ConstantConfigMap;
  // unique pointers to the dataset objects
  std::map<std::string, VariableConfig::VariableDataset> VariablePtrs;
  std::map<std::string, VariableConfig::ConstantDataset> ConstantPtrs;
};
} // namespace WriterModule::da00
