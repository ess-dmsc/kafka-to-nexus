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
  Logger->error("----------------EP00 init_hdf11111");

  return init_hdf(HDFGroup, &HDFAttributes,
                  CreateWriterTypedBaseMethod::CREATE);
}

void HDFWriterModule::parse_config(std::string const &ConfigurationStream,
                                   std::string const &ConfigurationModule) {
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
  Logger->error("----------------EP00 reopen");
  Timestamp = h5::h5d_chunked_1d<uint64_t>::open(HDFGroup, "timestamp");
  return init_hdf(HDFGroup, nullptr, CreateWriterTypedBaseMethod::OPEN);
}

HDFWriterModule::InitResult
HDFWriterModule::init_hdf(hdf5::node::Group &HDFGroup,
                          std::string const *HDFAttributes,
                          CreateWriterTypedBaseMethod CreateMethod) {
  Logger->error("----------------EP00 init_hdf22222");

  try {
    if (CreateMethod == CreateWriterTypedBaseMethod::CREATE) {
      if (HDFGroup.attributes.exists("NX_class")) {
        Logger->info("NX_class already specified!");
      } else {
        auto ClassAttribute =
            HDFGroup.attributes.create<std::string>("NX_class");
        ClassAttribute.write("NXlog");
      }
      this->Timestamp =
          h5::h5d_chunked_1d<uint64_t>::create(HDFGroup, "timestamp", 6);
      auto AttributesJson = nlohmann::json::parse(*HDFAttributes);
      writeAttributes(HDFGroup, &AttributesJson, Logger);
    } else if (CreateMethod == CreateWriterTypedBaseMethod::OPEN) {
      Logger->error("initializing timestamp");
      Logger->error(HDFGroup.link().path().name());
      this->Timestamp =
          h5::h5d_chunked_1d<uint64_t>::open(HDFGroup, "timestamp");
      this->Value = h5::Chunked1DString::create(HDFGroup, "status", 6);
      Timestamp->buffer_init(buffer_size, buffer_packet_max);
      Logger->error("initialized timestamp");
    }
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "f142 could not init HDFGroup: {}  trace: {}",
        static_cast<std::string>(HDFGroup.link().path()), message)));
  }
  return HDFWriterModule::InitResult::OK;
}

void HDFWriterModule::write(FlatbufferMessage const &Message) {
  Logger->error("----------------EP00 writeeee");
  auto fbuf = get_fbuf(Message.data());
  std::string Type = EnumNameEventType(fbuf->type());
  Logger->error("{}____{}", Type, Type.c_str());
  this->Value->append(Type);
  auto tmstmp = fbuf->timestamp();
  //  const unsigned long a = 255;
  this->Timestamp->append_data_1d(&tmstmp, 1);
  Logger->error("source: {}, timestamp: {}, type: {}.",
                fbuf->source_name()->str(), fbuf->timestamp(),
                EnumNameEventType(fbuf->type()));
}

int32_t HDFWriterModule::flush() { return 0; }

int32_t HDFWriterModule::close() {
  Timestamp.reset();
  return 0;
}

HDFWriterModule::HDFWriterModule() {
  Logger->error("----------------EP00 construct");
}

bool HDFWriterModule::findType(const nlohmann::basic_json<> Attribute,
                               std::string &DType) {
  if (auto AttrType = find<std::string>("type", Attribute)) {
    DType = AttrType.inner();
    return true;
  } else if (auto AttrType = find<std::string>("dtype", Attribute)) {
    DType = AttrType.inner();
    return true;
  } else
    return false;
}

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter