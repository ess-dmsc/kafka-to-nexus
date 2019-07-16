#include "ep00_rw.h"

namespace FileWriter {
namespace Schemas {
namespace ep00 {

static HDFWriterModuleRegistry::Registrar<HDFWriterModule>
    RegisterWriter("ep00");

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup,
                          std::string const &HDFAttributes) {
  Logger->error("----------------EP00 init_hdf1");
  return HDFWriterModule::InitResult::ERROR;
}

void HDFWriterModule::parse_config(std::string const &ConfigurationStream,
                                   std::string const &ConfigurationModule) {
  Logger->error("----------------EP00 parse config");
}

HDFWriterModule::InitResult
HDFWriterModule::reopen(hdf5::node::Group &HDFGroup) {
  Logger->error("----------------EP00 parse config");

  return HDFWriterModule::InitResult::ERROR;
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup,
                          std::string const *HDFAttributes,
                          CreateWriterTypedBaseMethod CreateMethod) {
  Logger->error("----------------EP00 init_hdf2");

  return HDFWriterModule::InitResult::ERROR;
}

void HDFWriterModule::write(FlatbufferMessage const &Message) {}

int32_t HDFWriterModule::flush() { return 0; }

int32_t HDFWriterModule::close() { return 0; }

HDFWriterModule::HDFWriterModule() {
  Logger->error("----------------EP00 construct");
}
} // namespace ep00
} // namespace Schemas
} // namespace FileWriter