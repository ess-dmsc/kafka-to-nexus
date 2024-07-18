// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "al00_Writer.h"
#include "MetaData/HDF5DataWriter.h"
#include "WriterRegistrar.h"
#include "json.h"
#include "logger.h"
#include <al00_alarm_generated.h>
#include <algorithm>
#include <cctype>

namespace WriterModule::al00 {

/// \brief Implement the writer module interface, forward to the CREATE case
/// of
/// `init_hdf`.
InitResult al00_Writer::init_hdf(hdf5::node::Group &HDFGroup) {
  auto Create = NeXusDataset::Mode::Create;
  try {
    NeXusDataset::AlarmMsg(HDFGroup, Create);
    NeXusDataset::AlarmTime(HDFGroup, Create);
    NeXusDataset::AlarmSeverity(HDFGroup, Create);
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    Logger::Error("al00 could not init_hdf hdf_parent: {}  trace: {}",
                  static_cast<std::string>(HDFGroup.link().path()), message);
    return InitResult::ERROR;
  }

  return InitResult::OK;
}

/// \brief Implement the writer module interface, forward to the OPEN case of
/// `init_hdf`.
InitResult al00_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    AlarmTime = NeXusDataset::AlarmTime(HDFGroup, Open);
    AlarmSeverity = NeXusDataset::AlarmSeverity(HDFGroup, Open);
    AlarmMsg = NeXusDataset::AlarmMsg(HDFGroup, Open);
  } catch (std::exception &E) {
    Logger::Error(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

void al00_Writer::writeImpl(FlatbufferMessage const &Message) {
  auto AlarmMessage = GetAlarm(Message.data());

  AlarmTime.appendElement(AlarmMessage->timestamp());
  AlarmSeverity.appendElement(
      static_cast<std::int16_t>(AlarmMessage->severity()));
  std::string AlarmMessageString = AlarmMessage->message()->str();
  if (AlarmMessageString.empty()) {
    AlarmMessageString = "NO ALARM MESSAGE";
  }
  AlarmMsg.appendStringElement(AlarmMessageString);
}

/// Register the writer module.
static WriterModule::Registry::Registrar<al00_Writer>
    RegisterWriter("al00", "alarm_info");

} // namespace WriterModule::al00
