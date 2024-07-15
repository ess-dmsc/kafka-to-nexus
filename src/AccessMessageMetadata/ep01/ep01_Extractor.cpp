#include "ep01_Extractor.h"
#include <ep01_epics_connection_generated.h>

namespace AccessMessageMetadata {

bool ep01_Extractor::verify(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(Message.data(), Message.size());
  return VerifyEpicsPVConnectionInfoBuffer(Verifier);
}

std::string ep01_Extractor::source_name(
    FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetEpicsPVConnectionInfo(Message.data());
  auto SourceName = FBuffer->source_name();
  if (SourceName == nullptr) {
    Logger::Info("Message has no source name.");
    return "";
  }
  return SourceName->str();
}

uint64_t
ep01_Extractor::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetEpicsPVConnectionInfo(Message.data());
  return FBuffer->timestamp();
}

static FileWriter::FlatbufferReaderRegistry::Registrar<ep01_Extractor>
    RegisterReader("ep01");
} // namespace AccessMessageMetadata
