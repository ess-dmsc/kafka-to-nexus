#pragma once

#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include "../../h5.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

class Reader : public FileWriter::FlatbufferReader {
  bool verify(Msg const &msg) const override;
  std::string source_name(Msg const &msg) const override;
  uint64_t timestamp(Msg const &msg) const override;
};
}
}
}
