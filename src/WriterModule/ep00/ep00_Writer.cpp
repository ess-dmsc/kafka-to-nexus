#include "ep00_Writer.h"
#include "FlatbufferMessage.h"
#include "WriterRegistrar.h"
#include <ep00_epics_connection_info_generated.h>

namespace WriterModule {
namespace ep00 {

InitResult ep00_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    TimestampDataset = NeXusDataset::ConnectionStatusTime(HDFGroup, Open);
    StatusDataset = NeXusDataset::ConnectionStatus(HDFGroup, Open);
  } catch (std::exception &E) {
    LOG_ERROR(
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

InitResult ep00_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  auto Create = NeXusDataset::Mode::Create;
  try {
    NeXusDataset::ConnectionStatusTime(
        HDFGroup, Create, ChunkSize); // NOLINT(bugprone-unused-raii)
    NeXusDataset::ConnectionStatus(HDFGroup, Create,
                                   ChunkSize); // NOLINT(bugprone-unused-raii)
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("ep00 could not init_hdf HDFGroup: {}  trace: {}",
                  static_cast<std::string>(HDFGroup.link().path()), message);
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

void ep00_Writer::write(FileWriter::FlatbufferMessage const &Message) {
  auto FlatBuffer = GetEpicsConnectionInfo(Message.data());
  std::string const Status = EnumNameEventType(FlatBuffer->type());
  StatusDataset.appendStringElement(Status);
  auto FBTimestamp = FlatBuffer->timestamp();
  TimestampDataset.appendElement(FBTimestamp);
}

static WriterModule::Registry::Registrar<ep00_Writer> RegisterWriter("ep00",
                                                                     "ep00");

} // namespace ep00
} // namespace WriterModule
