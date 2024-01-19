// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FlatbufferMessage.h"
#include "JsonConfig/Field.h"
#include "MetaData/Value.h"
#include "WriterModuleBase.h"
#include <NeXusDataset/EpicsAlarmDatasets.h>
#include <NeXusDataset/NeXusDataset.h>
#include <array>
#include <chrono>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <vector>

namespace WriterModule {
namespace al00 {
using FlatbufferMessage = FileWriter::FlatbufferMessage;

class al00_Writer : public WriterModule::Base {
public:
  /// Implements writer module interface.
  InitResult init_hdf(hdf5::node::Group &HDFGroup) const override;

  WriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;

  /// Write an incoming message which should contain a flatbuffer.
  void writeImpl(FlatbufferMessage const &Message) override;

  al00_Writer() : WriterModule::Base("al00", false, "NXlog") {}
  ~al00_Writer() override = default;

protected:
  NeXusDataset::AlarmTime AlarmTime;
  NeXusDataset::AlarmSeverity AlarmSeverity;
  NeXusDataset::AlarmMsg AlarmMsg;
};

} // namespace al00
} // namespace WriterModule
