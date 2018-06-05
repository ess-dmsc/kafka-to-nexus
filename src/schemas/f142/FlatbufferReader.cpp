#include "FlatbufferReader.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

FBUF const *get_fbuf(char const *data) { return GetLogData(data); }

bool FlatbufferReader::verify(Msg const &msg) const {
  auto veri = flatbuffers::Verifier((uint8_t *)msg.data(), msg.size());
  return VerifyLogDataBuffer(veri);
}

std::string FlatbufferReader::source_name(Msg const &msg) const {
  auto fbuf = get_fbuf(msg.data());
  auto s1 = fbuf->source_name();
  if (!s1) {
    LOG(Sev::Warning, "message has no source name");
    return "";
  }
  return s1->str();
}

uint64_t FlatbufferReader::timestamp(Msg const &msg) const {
  auto fbuf = get_fbuf(msg.data());
  return fbuf->timestamp();
}

static FlatbufferReaderRegistry::Registrar<FlatbufferReader>
    RegisterReader("f142");
}
}
}
