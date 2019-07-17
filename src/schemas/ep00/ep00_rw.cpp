#include "ep00_rw.h"
#include <HDFFile.h>
#include <json.h>

namespace FileWriter {
namespace Schemas {
namespace ep00 {
using nlohmann::json;
static HDFWriterModuleRegistry::Registrar<HDFWriterModule>
    RegisterWriter("ep00");

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup,
                          std::string const &HDFAttributes) {
  return init_hdf(HDFGroup, &HDFAttributes,
                  CreateWriterTypedBaseMethod::CREATE);
}

void HDFWriterModule::parse_config(
    std::string const &ConfigurationStream,
    std::string const & /*ConfigurationModule*/) {
  auto ConfigurationStreamJson = json::parse(ConfigurationStream);
  if (auto SourceNameMaybe =
          find<std::string>("source", ConfigurationStreamJson)) {
    SourceName = SourceNameMaybe.inner();
  } else {
    Logger->error("Key \"source\" is not specified in ep00 json command");
    return;
  }

  Logger->error("----------------EP00 parse config");
}

HDFWriterModule::InitResult
HDFWriterModule::reopen(hdf5::node::Group &HDFGroup) {
  return init_hdf(HDFGroup, nullptr, CreateWriterTypedBaseMethod::OPEN);
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup,
                          std::string const *HDFAttributes,
                          CreateWriterTypedBaseMethod CreateMethod) {
  const std::string StatusName = "alarm_status";
  const std::string TimestampName = "alarm_time";

  try {
    if (CreateMethod == CreateWriterTypedBaseMethod::CREATE) {
      if (HDFGroup.attributes.exists("NX_class")) {
        Logger->info("NX_class already specified!");
      } else {
        auto ClassAttribute =
            HDFGroup.attributes.create<std::string>("NX_class");
        ClassAttribute.write("NXlog");
      }
      AlarmTimestamp = h5::h5d_chunked_1d<uint64_t>::create(
          HDFGroup, TimestampName, ChunkBytes);
      AlarmStatus =
          h5::Chunked1DString::create(HDFGroup, StatusName, ChunkBytes);
      auto AttributesJson = nlohmann::json::parse(*HDFAttributes);
      writeAttributes(HDFGroup, &AttributesJson, Logger);
    } else if (CreateMethod == CreateWriterTypedBaseMethod::OPEN) {
      AlarmStatus = h5::Chunked1DString::open(HDFGroup, StatusName);
      AlarmTimestamp =
          h5::h5d_chunked_1d<uint64_t>::open(HDFGroup, TimestampName);

      AlarmTimestamp->buffer_init(BufferSize, BufferPacketMax);
    }
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "ep00 could not init HDFGroup: {}  trace: {}",
        static_cast<std::string>(HDFGroup.link().path()), message)));
  }
  return HDFWriterModule::InitResult::OK;
}

void HDFWriterModule::write(FlatbufferMessage const &Message) {
  auto FlatBuffer = get_fbuf(Message.data());
  std::string Status = EnumNameEventType(FlatBuffer->type());
  this->AlarmStatus->append(Status);
  auto FBTimestamp = FlatBuffer->timestamp();
  this->AlarmTimestamp->append_data_1d(&FBTimestamp, 1);
}

int32_t HDFWriterModule::flush() { return 0; }

int32_t HDFWriterModule::close() {
  AlarmTimestamp.reset();
  return 0;
}

HDFWriterModule::HDFWriterModule() {}

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter