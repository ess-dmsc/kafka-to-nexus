#include "vs00_Extractor.h"
#include <vs00_stringdata_generated.h>

namespace AccessMessageMetadata {

bool vs00_Extractor::verify(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(Message.data(), Message.size());
  return Verifyvs00_StringDataBuffer(Verifier);
}

std::string vs00_Extractor::source_name(
    FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = Getvs00_StringData(Message.data());
  auto SourceName = FBuffer->source_name();
  if (SourceName == nullptr) {
    Logger::Info("Message has no source name.");
    return "";
  }
  return SourceName->str();
}

int64_t
vs00_Extractor::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = Getvs00_StringData(Message.data());
  return FBuffer->timestamp();
}

static FileWriter::FlatbufferReaderRegistry::Registrar<vs00_Extractor>
    RegisterReader("vs00");
} // namespace AccessMessageMetadata
