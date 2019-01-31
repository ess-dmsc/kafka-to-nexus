#include "FlatbufferReader.h"

namespace FileWriter {
namespace Schemas {
namespace f142 {

/// Cast byte blob to flatbuffer message
FBUF const *get_fbuf(char const *data) { return GetLogData(data); }

/// \brief  Use flatbuffers library to validate a message
bool FlatbufferReader::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyLogDataBuffer(Verifier);
}

/// Extract name of source from the message
std::string
FlatbufferReader::source_name(FlatbufferMessage const &Message) const {
  auto fbuf = get_fbuf(Message.data());
  auto s1 = fbuf->source_name();
  if (s1 == nullptr) {
    LOG(spdlog::level::warn, "message has no source name");
    return "";
  }
  return s1->str();
}

/// Extract timestamp from the message
uint64_t FlatbufferReader::timestamp(FlatbufferMessage const &Message) const {
  auto fbuf = get_fbuf(Message.data());
  return fbuf->timestamp();
}

/// Register the Reader with the application's registry
static FlatbufferReaderRegistry::Registrar<FlatbufferReader>
    RegisterReader("f142");
} // namespace f142
} // namespace Schemas
} // namespace FileWriter
