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

  try {
    ChunkBytes =
        ConfigurationStreamJson["nexus"]["chunk"]["chunk_kb"].get<uint64_t>() *
        1024;
    Logger->trace("chunk_bytes: {}", ChunkBytes);
  } catch (...) { /* it's ok if not found */
  }
  try {
    ChunkBytes =
        ConfigurationStreamJson["nexus"]["chunk"]["chunk_mb"].get<uint64_t>() *
        1024 * 1024;
    Logger->trace("chunk_bytes: {}", ChunkBytes);
  } catch (...) { /* it's ok if not found */
  }
  try {
    BufferSize =
        ConfigurationStreamJson["nexus"]["buffer"]["size_kb"].get<uint64_t>() *
        1024;
    Logger->trace("buffer_size: {}", BufferSize);
  } catch (...) { /* it's ok if not found */
  }
  try {
    BufferSize =
        ConfigurationStreamJson["nexus"]["buffer"]["size_mb"].get<uint64_t>() *
        1024 * 1024;
    Logger->trace("buffer_size: {}", BufferSize);
  } catch (...) { /* it's ok if not found */
  }
  try {
    BufferPacketMax =
        ConfigurationStreamJson["nexus"]["buffer"]["packet_max_kb"]
            .get<uint64_t>() *
        1024;
    Logger->trace("buffer_packet_max: {}", BufferPacketMax);
  } catch (...) { /* it's ok if not found */
  }
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
      if (AlarmStatus == nullptr) {
        throw std::runtime_error("Cannot create ep00 AlarmStatus.");
      }
      if (AlarmTimestamp == nullptr) {
        throw std::runtime_error("Cannot create ep00 AlarmTimestamp.");
      }
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
    Logger->error("ep00 could not init HDFGroup: {}  trace: {}",
                  static_cast<std::string>(HDFGroup.link().path()), message);
    return HDFWriterModule::InitResult::ERROR;
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

int32_t HDFWriterModule::close() { return 0; }

HDFWriterModule::HDFWriterModule() {}

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
