// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "mdat_Writer.h"
#include "WriterRegistrar.h"

namespace mdat {

// Register our module so the parsing of JSON recognises it.
static WriterModule::Registry::Registrar<mdat_Writer> RegisterWriter("mdat", "mdat");

WriterModule::InitResult mdat_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  try {
    NeXusDataset::Time(HDFGroup, NeXusDataset::Mode::Create); //  make a timestamp entry in ns since epoch
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("mdat could not init_hdf (or re-open) HDFGroup: {}  trace: {}\nError with NeXusDataset::Time(..)?",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return WriterModule::InitResult::ERROR;
  }

  return WriterModule::InitResult::OK;

}

WriterModule::InitResult mdat_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    mdatStart_time = NeXusDataset::Time(HDFGroup, NeXusDataset::Mode::Open);
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("mdat could not init_hdf (or re-open) HDFGroup: {}  trace: {}\nError with NeXusDataset::Time(..)?",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return WriterModule::InitResult::ERROR;
  }

  return WriterModule::InitResult::OK;

}

} // namespace mdat
