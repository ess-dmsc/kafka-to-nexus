#include "pvAl_Extractor.h"
#include <al00_alarm_generated.h>

namespace AccessMessageMetadata {

bool al00_Extractor::verify(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(Message.data(), Message.size());
  return VerifyAlarmBuffer(Verifier);
}

std::string al00_Extractor::source_name(
    FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetAlarm(Message.data());
  auto SourceName = FBuffer->source_name();
  if (SourceName == nullptr) {
    LOG_WARN("Message has no source name.");
    return "";
  }
  return SourceName->str();
}

uint64_t
al00_Extractor::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetAlarm(Message.data());
  return FBuffer->timestamp();
}

static FileWriter::FlatbufferReaderRegistry::Registrar<al00_Extractor>
    RegisterReader("al00");
} // namespace AccessMessageMetadata
