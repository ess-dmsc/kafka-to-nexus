#include "f144_Extractor.h"
#include <f144_logdata_generated.h>

namespace AccessMessageMetadata {

bool f144_Extractor::verify(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(Message.data(), Message.size());
  return Verifyf144_LogDataBuffer(Verifier);
}

std::string f144_Extractor::source_name(
    FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = Getf144_LogData(Message.data());
  auto SourceName = FBuffer->source_name();
  if (SourceName == nullptr) {
    Logger::Info("Message has no source name.");
    return "";
  }
  return SourceName->str();
}

uint64_t
f144_Extractor::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = Getf144_LogData(Message.data());
  return FBuffer->timestamp();
}

static FileWriter::FlatbufferReaderRegistry::Registrar<f144_Extractor>
    RegisterReader("f144");
} // namespace AccessMessageMetadata
