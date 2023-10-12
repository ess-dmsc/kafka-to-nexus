// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "mdat_Writer.h"
#include "HDFOperations.h"
#include "WriterRegistrar.h"

namespace WriterModule::mdat {

// Register our module so the parsing of JSON recognises it.
static WriterModule::Registry::Registrar<mdat_Writer> RegisterWriter("mdat",
                                                                     "mdat");

WriterModule::InitResult
mdat_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  try {
    NeXusDataset::DateTime(HDFGroup, "start_time", NeXusDataset::Mode::Create,
                           StringSize, ChunkSize);
    NeXusDataset::DateTime(HDFGroup, "end_time", NeXusDataset::Mode::Create,
                           StringSize, ChunkSize);
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
    mdatStart_datetime =
        NeXusDataset::DateTime(HDFGroup, "start_time", NeXusDataset::Mode::Open,
                               StringSize, ChunkSize);
    mdatEnd_datetime = NeXusDataset::DateTime(
        HDFGroup, "end_time", NeXusDataset::Mode::Open, StringSize, ChunkSize);
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("mdat could not reopen HDFGroup: {}  trace: {}\nError with "
              "NeXusDataset::DateTime(..)?",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return WriterModule::InitResult::ERROR;
  }

  return WriterModule::InitResult::OK;
}

void mdat_Writer::writeStartTime(time_point startTime) {
  mdatStart_datetime.appendStringElement(convertToIso8601(startTime));
}

void mdat_Writer::writeStopTime(time_point stopTime) {
  mdatEnd_datetime.appendStringElement(convertToIso8601(stopTime));
}

std::string mdat_Writer::convertToIso8601(time_point timePoint) {
  char buffer[32];
  time_t datatime = std::chrono::system_clock::to_time_t(timePoint);
  tm *nowtm = gmtime(&datatime);
  std::strftime(buffer, max_buffer_length, "%FT%TZ", nowtm);
  return {buffer};
}


} // namespace WriterModule::mdat
