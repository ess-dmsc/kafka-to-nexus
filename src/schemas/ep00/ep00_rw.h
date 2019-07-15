#pragma once

#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include <ep00_epics_connection_info_generated.h>

namespace FileWriter {
namespace Schemas {
namespace ep00 {

using FBUF = EpicsConnectionInfo;
FBUF const *get_fbuf(char const *data);

class FlatbufferReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;
};

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
