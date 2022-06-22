#include "pvCn_Extractor.h"
#include <pvCn_epics_connection_generated.h>

namespace AccessMessageMetadata {

bool pvCn_Extractor::verify(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(Message.data(), Message.size());
  return VerifyEpicsPVConnectionInfoBuffer(Verifier);
}

std::string pvCn_Extractor::source_name(
    FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetEpicsPVConnectionInfo(Message.data());
  auto SourceName = FBuffer->source_name();
  if (SourceName == nullptr) {
    LOG_WARN("Message has no source name.");
    return "";
  }
  return SourceName->str();
}

uint64_t
pvCn_Extractor::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetEpicsPVConnectionInfo(Message.data());
  return FBuffer->timestamp();
}

static FileWriter::FlatbufferReaderRegistry::Registrar<pvCn_Extractor>
    RegisterReader("pvCn");
} // namespace AccessMessageMetadata
