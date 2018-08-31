#pragma once

#include "../../FlatbufferMessage.h"
#include "../../FlatbufferReader.h"
#include "Common.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

FBUF const *get_fbuf(char const *data);

class FlatbufferReader : public FileWriter::FlatbufferReader {
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;
};
}
}
}
