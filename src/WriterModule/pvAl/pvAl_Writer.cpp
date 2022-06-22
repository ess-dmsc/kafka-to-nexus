// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "pvAl_Writer.h"
#include "MetaData/HDF5DataWriter.h"
#include "WriterRegistrar.h"
#include "json.h"
#include "logger.h"
#include <algorithm>
#include <cctype>
#include <pvAl_epics_pv_alarm_state_generated.h>

namespace WriterModule::pvAl {

/// \brief Implement the writer module interface, forward to the CREATE case
/// of
/// `init_hdf`.
InitResult pvAl_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  auto Create = NeXusDataset::Mode::Create;
  try {
    NeXusDataset::AlarmTime(HDFGroup, Create);
    NeXusDataset::AlarmStatus(HDFGroup, Create);
    NeXusDataset::AlarmSeverity(HDFGroup, Create);
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("pvAl could not init_hdf hdf_parent: {}  trace: {}",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return InitResult::ERROR;
  }

  return InitResult::OK;
}

/// \brief Implement the writer module interface, forward to the OPEN case of
/// `init_hdf`.
InitResult pvAl_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    AlarmTime = NeXusDataset::AlarmTime(HDFGroup, Open);
    AlarmStatus = NeXusDataset::AlarmStatus(HDFGroup, Open);
    AlarmSeverity = NeXusDataset::AlarmSeverity(HDFGroup, Open);
  } catch (std::exception &E) {
    LOG_ERROR(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

void pvAl_Writer::write(FlatbufferMessage const &Message) {
  auto AlarmMessage = GetPV_AlarmState(Message.data());

  AlarmTime.appendElement(AlarmMessage->timestamp());

  auto AlarmStateString =
      std::string(EnumNameAlarmState(AlarmMessage->state()));
  if (AlarmStateString.empty()) {
    AlarmStateString = "UNRECOGNISED_STATE";
  }
  AlarmStatus.appendStringElement(AlarmStateString);

  auto AlarmSeverityString =
      std::string(EnumNameAlarmSeverity(AlarmMessage->severity()));
  if (AlarmSeverityString.empty()) {
    AlarmSeverityString = "UNRECOGNISED_SEVERITY";
  }
  AlarmSeverity.appendStringElement(AlarmSeverityString);
}

/// Register the writer module.
static WriterModule::Registry::Registrar<pvAl_Writer>
    RegisterWriter("pvAl", "epics_alarm_status");

} // namespace WriterModule::pvAl
