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
namespace pvAl {
using FlatbufferMessage = FileWriter::FlatbufferMessage;

class pvAl_Writer : public WriterModule::Base {
public:
  /// Implements writer module interface.
  InitResult init_hdf(hdf5::node::Group &HDFGroup) const override;

  WriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;

  /// Write an incoming message which should contain a flatbuffer.
  void write(FlatbufferMessage const &Message) override;

  pvAl_Writer() : WriterModule::Base(false, "NXlog") {}
  ~pvAl_Writer() override = default;

protected:
  /// Timestamps of changes in EPICS alarm status
  NeXusDataset::AlarmTime AlarmTime;

  /// Changes in EPICS alarm status
  NeXusDataset::AlarmStatus AlarmStatus;

  /// Severity corresponding to EPICS alarm status
  NeXusDataset::AlarmSeverity AlarmSeverity;
};

} // namespace pvAl
} // namespace WriterModule
