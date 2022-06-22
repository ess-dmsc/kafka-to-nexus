#include "pvAl_Extractor.h"
#include <pvAl_epics_pv_alarm_state_generated.h>

namespace AccessMessageMetadata {

bool pvAl_Extractor::verify(
    FileWriter::FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(Message.data(), Message.size());
  return VerifyPV_AlarmStateBuffer(Verifier);
}

std::string pvAl_Extractor::source_name(
    FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetPV_AlarmState(Message.data());
  auto SourceName = FBuffer->source_name();
  if (SourceName == nullptr) {
    LOG_WARN("Message has no source name.");
    return "";
  }
  return SourceName->str();
}

uint64_t
pvAl_Extractor::timestamp(FileWriter::FlatbufferMessage const &Message) const {
  auto FBuffer = GetPV_AlarmState(Message.data());
  return FBuffer->timestamp();
}

static FileWriter::FlatbufferReaderRegistry::Registrar<pvAl_Extractor>
RegisterReader("pvAl");
} // namespace AccessMessageMetadata
