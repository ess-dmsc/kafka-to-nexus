// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "vs00_Writer.h"
#include "MetaData/HDF5DataWriter.h"
#include "WriterRegistrar.h"
#include "json.h"
#include "logger.h"
#include <vs00_stringdata_generated.h>
#include <algorithm>
#include <cctype>

namespace WriterModule::vs00 {

/// \brief Implement the writer module interface, forward to the CREATE case
/// of
/// `init_hdf`.
InitResult vs00_Writer::init_hdf(hdf5::node::Group &HDFGroup) {
  auto Create = NeXusDataset::Mode::Create;
  try {
    NeXusDataset::FixedSizeString(HDFGroup, Create);
    NeXusDataset::Time(HDFGroup, Create);
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    Logger::Error("vs00 could not init_hdf hdf_parent: {}  trace: {}",
                  static_cast<std::string>(HDFGroup.link().path()), message);
    return InitResult::ERROR;
  }

  return InitResult::OK;
}

/// \brief Implement the writer module interface, forward to the OPEN case of
/// `init_hdf`.
InitResult vs00_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    Timestamp = NeXusDataset::Time(HDFGroup, Open);
    Valie = NeXusDataset::FixedSizeString(HDFGroup, Open);
  } catch (std::exception &E) {
    Logger::Error(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

bool vs00_Writer::writeImpl(FlatbufferMessage const &Message,
                            [[maybe_unused]] bool is_buffered_message) {
  auto vs00Message = Getvs00_StringData(Message.data());

  Timestamp.appendElement(vs00Message->timestamp());
  std::string vs00MessageValue = vs00Message->message()->str();
  if (vs00MessageValue.empty()) {
    return false;
  }
  Value.appendStringElement(vs00MessageValue);
  return true;
}

/// Register the writer module.
static WriterModule::Registry::Registrar<vs00_Writer>
    RegisterWriter("vs00", "vs00");

} // namespace WriterModule::vs00
