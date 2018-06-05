#pragma once

#include "../../FlatbufferReader.h"
#include "Common.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

FBUF const *get_fbuf(char const *data);

class FlatbufferReader : public FileWriter::FlatbufferReader {
  bool verify(Msg const &msg) const override;
  std::string source_name(Msg const &msg) const override;
  uint64_t timestamp(Msg const &msg) const override;
};
}
}
}
