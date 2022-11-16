#include "scal_Extractor.h"
#include <f144_logdata_generated.h>

namespace AccessMessageMetadata {

bool f144_Extractor::verify(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(Message.data(), Message.size());
  return VerifyLogDataBuffer(Verifier);
}

std::string f144_Extractor::source_name(
    FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetLogData(Message.data());
  auto SourceName = FBuffer->source_name();
  if (SourceName == nullptr) {
    LOG_WARN("Message has no source name.");
    return "";
  }
  return SourceName->str();
}

uint64_t
f144_Extractor::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetLogData(Message.data());
  return FBuffer->timestamp();
}

static FileWriter::FlatbufferReaderRegistry::Registrar<f144_Extractor>
    RegisterReader("f144");
} // namespace AccessMessageMetadata
