#include "ep00_rw.h"
#include "HDFFile.h"
#include "json.h"

namespace FileWriter {
namespace Schemas {
namespace ep00 {
using nlohmann::json;
static HDFWriterModuleRegistry::Registrar<HDFWriterModule>
    RegisterWriter("ep00");

void HDFWriterModule::parse_config(std::string const &ConfigurationStream) {
  // This writer module has no additional options to parse
  UNUSED_ARG(ConfigurationStream);
}

HDFWriterModule::InitResult
HDFWriterModule::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    TimestampDataset = NeXusDataset::ConnectionStatusTime(HDFGroup, Open);
    StatusDataset = NeXusDataset::ConnectionStatus(HDFGroup, Open);
  } catch (std::exception &E) {
    Logger->error(
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return HDFWriterModule::InitResult::ERROR;
  }
  return HDFWriterModule::InitResult::OK;
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup, std::string const &) {
  auto Create = NeXusDataset::Mode::Create;
  try {
    NeXusDataset::ConnectionStatusTime(HDFGroup,
                                       Create); // NOLINT(bugprone-unused-raii)
    NeXusDataset::ConnectionStatus(HDFGroup,
                                   Create); // NOLINT(bugprone-unused-raii)
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    Logger->error("ep00 could not init HDFGroup: {}  trace: {}",
                  static_cast<std::string>(HDFGroup.link().path()), message);
    return HDFWriterModule::InitResult::ERROR;
  }
  return HDFWriterModule::InitResult::OK;
}

void HDFWriterModule::write(FlatbufferMessage const &Message) {
  auto FlatBuffer = get_fbuf(Message.data());
  std::string Status = EnumNameEventType(FlatBuffer->type());
  StatusDataset.appendString(Status);
  auto FBTimestamp = FlatBuffer->timestamp();
  TimestampDataset.appendElement(FBTimestamp);
}

int32_t HDFWriterModule::close() { return 0; }

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
