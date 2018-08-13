#include "FlatbufferMessage.h"
#include "FlatbufferReader.h"

namespace FileWriter {
  FlatbufferMessage::FlatbufferMessage() : Valid(false) {
  }
  
  FlatbufferMessage::FlatbufferMessage(char const *Pointer, size_t const Size) : DataPtr(const_cast<char*>(Pointer)), DataSize(Size) {
    extractPacketInfo();
  }
  
  void FlatbufferMessage::extractPacketInfo() {
    if (DataSize < 8) {
      Valid = false;
      throw BufferTooSmallError(fmt::format("Flatbuffer was only {} bytes. Expected ≥ 8 bytes."));
    }
    std::string FlatbufferID(data() + 4, 4);
    try {
      auto &Reader = FlatbufferReaderRegistry::find(FlatbufferID);
      Sourcename = Reader->source_name(*this);
      Timestamp = Reader->timestamp(*this);
    } catch (std::out_of_range &E) {
      Valid = false;
      throw UnknownFlatbufferID(fmt::format("Unable to locate reader with the ID \"{}\" in the registry.", FlatbufferID));
    }
    Valid = true;
  }
  
  FlatbufferMessage::~FlatbufferMessage() {
  }
}

