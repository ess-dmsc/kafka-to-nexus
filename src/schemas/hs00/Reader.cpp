#include "Reader.h"
#include "logger.h"
#include <flatbuffers/flatbuffers.h>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

#include "hs00_event_histogram_generated.h"

static EventHistogram const *getRoot(char const *Data) {
  return GetEventHistogram(Data);
}

bool Reader::verify(FlatbufferMessage const &Message) const {
  flatbuffers::Verifier Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  return VerifyEventHistogramBuffer(Verifier);
}

std::string Reader::source_name(FlatbufferMessage const &Message) const {
  auto Buffer = getRoot(Message.data());
  auto Source = Buffer->source();
  if (Source == nullptr) {
    spdlog::get("filewriterlogger")->info("message has no source_name");
    return "";
  }
  return Source->str();
}

uint64_t Reader::timestamp(FlatbufferMessage const &Message) const {
  auto Buffer = getRoot(Message.data());
  return Buffer->timestamp();
}

FlatbufferReaderRegistry::Registrar<Reader> RegisterReader("hs00");
} // namespace hs00
} // namespace Schemas
} // namespace FileWriter
