#include "FlatbufferMessage.h"
#include "FlatbufferReader.h"

namespace FileWriter {

FlatbufferMessage::FlatbufferMessage(char const *const BufferPtr,
                                     size_t const Size)
    : DataPtr(BufferPtr), DataSize(Size) {
  extractPacketInfo();
}

void FlatbufferMessage::extractPacketInfo() {
  if (DataSize < 8) {
    Valid = false;
    throw BufferTooSmallError(fmt::format(
        "Flatbuffer was only {} bytes. Expected ≥ 8 bytes.", DataSize));
  }
  std::string FlatbufferID(data() + 4, 4);
  try {
    auto &Reader = FlatbufferReaderRegistry::find(FlatbufferID);
    if (not Reader->verify(*this)) {
      throw NotValidFlatbuffer(
          fmt::format("Buffer which has flatbuffer ID \"{}\" is not a valid "
                      "flatbuffer of this type.",
                      FlatbufferID));
    }
    Sourcename = Reader->source_name(*this);
    Timestamp = Reader->timestamp(*this);
  } catch (std::out_of_range &E) {
    Valid = false;
    throw UnknownFlatbufferID(fmt::format(
        "Unable to locate reader with the ID \"{}\" in the registry.",
        FlatbufferID));
  }
  Valid = true;
}
} //namespace FileWriter
