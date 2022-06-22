#include "scal_Extractor.h"
#include <scal_epics_scalar_data_generated.h>

namespace AccessMessageMetadata {

bool scal_Extractor::verify(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(Message.data(), Message.size());
  return VerifyScalarDataBuffer(Verifier);
}

std::string scal_Extractor::source_name(
    FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetScalarData(Message.data());
  auto SourceName = FBuffer->source_name();
  if (SourceName == nullptr) {
    LOG_WARN("Message has no source name.");
    return "";
  }
  return SourceName->str();
}

uint64_t
scal_Extractor::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetScalarData(Message.data());
  return FBuffer->timestamp();
}

static FileWriter::FlatbufferReaderRegistry::Registrar<scal_Extractor>
RegisterReader("scal");
} // namespace AccessMessageMetadata
