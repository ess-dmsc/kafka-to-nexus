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

namespace WriterModule::mdat {

// Register our module so the parsing of JSON recognises it.
static WriterModule::Registry::Registrar<mdat_Writer> RegisterWriter("mdat",
                                                                     "mdat");

WriterModule::InitResult
mdat_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  try {
    NeXusDataset::Time(HDFGroup, "start_time", NeXusDataset::Mode::Create,
                       ChunkSize, "ms");
    NeXusDataset::Time(HDFGroup, "stop_time", NeXusDataset::Mode::Create,
                       ChunkSize, "ms");
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("mdat could not init_hdf HDFGroup: {}  trace: {}\nError with "
              "NeXusDataset::Time(..)?",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return WriterModule::InitResult::ERROR;
  }

  return WriterModule::InitResult::OK;
}

WriterModule::InitResult mdat_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    mdatStart_time = NeXusDataset::Time(
        HDFGroup, "start_time", NeXusDataset::Mode::Open, ChunkSize, "ms");
    mdatStop_time = NeXusDataset::Time(
        HDFGroup, "stop_time", NeXusDataset::Mode::Open, ChunkSize, "ms");
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("mdat could not reopen HDFGroup: {}  trace: {}\nError with "
              "NeXusDataset::Time(..)?",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return WriterModule::InitResult::ERROR;
  }

  return WriterModule::InitResult::OK;
}

void mdat_Writer::write(FileWriter::FlatbufferMessage const &Message) {
  std::cout << "mdat writ" << std::endl;
  Message.isValid();
  auto dataPointer =
      GetRunStart(Message.data()); //  we get data from 'fake' pl72
  if (!std::strcmp(dataPointer->nexus_structure()->c_str(), "start_time"))
    mdatStart_time.appendElement(dataPointer->start_time());
  if (!std::strcmp(dataPointer->nexus_structure()->c_str(), "stop_time"))
    mdatStop_time.appendElement(dataPointer->stop_time());
}

} // namespace WriterModule::mdat
