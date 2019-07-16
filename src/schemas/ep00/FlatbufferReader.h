#pragma once

#include "../../FlatbufferReader.h"
#include <flatbuffers/flatbuffers.h>

namespace FileWriter {
namespace Schemas {
namespace ep00 {
#include <ep00_epics_connection_info_generated.h>

using FBUF = EpicsConnectionInfo;
FBUF const *get_fbuf(char const *Data);
enum class CreateWriterTypedBaseMethod { CREATE, OPEN };


class FlatbufferReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
};

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
