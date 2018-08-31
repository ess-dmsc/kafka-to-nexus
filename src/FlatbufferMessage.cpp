#include "FlatbufferMessage.h"
#include "FlatbufferReader.h"

namespace FileWriter {

FlatbufferMessage::FlatbufferMessage(
    char const *const BufferPtr, size_t const Size,
    std::map<std::string, std::unique_ptr<FlatbufferReader>> const &Readers)
    : DataPtr(BufferPtr), DataSize(Size), Valid(false) {
  extractPacketInfo(Readers);
}

void FlatbufferMessage::extractPacketInfo(
    std::map<std::string, std::unique_ptr<FlatbufferReader>> const &Readers) {
  if (DataSize < 8) {
    Valid = false;
    throw BufferTooSmallError(fmt::format(
        "Flatbuffer was only {} bytes. Expected ≥ 8 bytes.", DataSize));
  }
  std::string FlatbufferID(data() + 4, 4);
  try {
    auto ReaderMaybe = Readers.find(FlatbufferID);
    if (ReaderMaybe == Readers.end()) {
      Valid = false;
      throw UnknownFlatbufferID(fmt::format(
          "Unable to locate reader with the ID \"{}\" in the registry.",
          FlatbufferID));
    }
    auto &Reader = ReaderMaybe->second;
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
}
