#include "ep01_Writer.h"
#include "FlatbufferMessage.h"
#include "WriterRegistrar.h"
#include <ep01_epics_connection_generated.h>

namespace WriterModule {
namespace ep01 {

InitResult ep01_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    TimestampDataset = NeXusDataset::ConnectionStatusTime(HDFGroup, Open);
    StatusDataset = NeXusDataset::ConnectionStatus(HDFGroup, Open);
  } catch (std::exception &E) {
    LOG_ERROR(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

InitResult ep01_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  auto Create = NeXusDataset::Mode::Create;
  try {
    NeXusDataset::ConnectionStatusTime(HDFGroup,
                                       Create); // NOLINT(bugprone-unused-raii)
    NeXusDataset::ConnectionStatus(HDFGroup,
                                   Create); // NOLINT(bugprone-unused-raii)
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("ep01 could not init_hdf HDFGroup: {}  trace: {}",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

void ep01_Writer::write(FileWriter::FlatbufferMessage const &Message) {
  auto FlatBuffer = GetEpicsPVConnectionInfo(Message.data());
  std::int16_t Status = static_cast<std::int16_t>(FlatBuffer->status());
  StatusDataset.appendElement(Status);
  auto FBTimestamp = FlatBuffer->timestamp();
  TimestampDataset.appendElement(FBTimestamp);
}

static WriterModule::Registry::Registrar<ep01_Writer>
    RegisterWriter("ep01", "epics_con_info");

} // namespace ep01
} // namespace WriterModule
