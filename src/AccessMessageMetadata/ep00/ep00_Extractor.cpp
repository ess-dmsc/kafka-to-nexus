#include "ep00_Extractor.h"
#include <ep00_epics_connection_info_generated.h>

namespace AccessMessageMetadata {

bool ep00_Extractor::verify(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(Message.data(), Message.size());
  return VerifyEpicsConnectionInfoBuffer(Verifier);
}

std::string ep00_Extractor::source_name(
    FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetEpicsConnectionInfo(Message.data());
  auto SourceName = FBuffer->source_name();
  if (SourceName == nullptr) {
    LOG_WARN("Message has no source name.");
    return "";
  }
  return SourceName->str();
}

uint64_t
ep00_Extractor::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetEpicsConnectionInfo(Message.data());
  return FBuffer->timestamp();
}

static FileWriter::FlatbufferReaderRegistry::Registrar<ep00_Extractor>
    RegisterReader("ep00");
} // namespace AccessMessageMetadata
