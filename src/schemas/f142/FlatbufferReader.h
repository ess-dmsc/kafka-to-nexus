#pragma once

#include "../../FlatbufferReader.h"
#include "Common.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// Declare helper to extract flatbuffer data from byte blob
FBUF const *get_fbuf(char const *data);

/// Implement Reader interface for f142
class FlatbufferReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;
};
}
}
}
