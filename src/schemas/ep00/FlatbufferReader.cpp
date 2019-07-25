#include "FlatbufferReader.h"

namespace FileWriter {
namespace Schemas {
namespace ep00 {
/// Cast byte blob to flatbuffer message
FBUF const *get_fbuf(char const *Data) { return GetEpicsConnectionInfo(Data); }

bool FlatbufferReader::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyEpicsConnectionInfoBuffer(Verifier);
}

std::string
FlatbufferReader::source_name(FlatbufferMessage const &Message) const {
  auto FBuffer = get_fbuf(Message.data());
  auto SourceName = FBuffer->source_name();
  if (SourceName == nullptr) {
    Logger->warn("Message has no source name.");
    return "";
  }
  return SourceName->str();
}

uint64_t FlatbufferReader::timestamp(FlatbufferMessage const &Message) const {
  auto FBuffer = get_fbuf(Message.data());
  return FBuffer->timestamp();
}

static FlatbufferReaderRegistry::Registrar<FlatbufferReader>
    RegisterReader("ep00");
} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
