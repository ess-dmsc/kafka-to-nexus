#include "Reader.h"
#include <flatbuffers/flatbuffers.h>

namespace FileWriter {
namespace Schemas {
namespace hs00 {

#include "schemas/hs00_event_histogram_generated.h"

static EventHistogram const *getRoot(char const *Data) {
  return GetEventHistogram(Data);
}

bool Reader::verify(Msg const &Message) const {
  flatbuffers::Verifier Verifier((uint8_t *)Message.data(), Message.size());
  return VerifyEventHistogramBuffer(Verifier);
}

std::string Reader::source_name(Msg const &Message) const {
  auto Buffer = getRoot(Message.data());
  auto Source = Buffer->source();
  if (!Source) {
    LOG(Sev::Notice, "message has no source_name");
    return "";
  }
  return Source->str();
}

uint64_t Reader::timestamp(Msg const &Message) const {
  auto Buffer = getRoot(Message.data());
  return Buffer->timestamp();
}

static FlatbufferReaderRegistry::Registrar<Reader> RegisterReader("hs00");
}
}
}
