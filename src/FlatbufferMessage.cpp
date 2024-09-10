// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatbufferMessage.h"
#include "FlatbufferReader.h"

namespace FileWriter {

FlatbufferMessage::FlatbufferMessage(uint8_t const *BufferPtr, size_t Size)
    : DataPtr(std::make_unique<uint8_t[]>(Size)), DataSize(Size) {
  std::memcpy(DataPtr.get(), BufferPtr, DataSize);
  extractPacketInfo();
}

FlatbufferMessage::FlatbufferMessage(FileWriter::Msg const &KafkaMessage)
    : DataPtr(std::make_unique<uint8_t[]>(KafkaMessage.size())),
      DataSize(KafkaMessage.size()) {
  std::memcpy(DataPtr.get(), KafkaMessage.data(), DataSize);
  extractPacketInfo();
}

FlatbufferMessage::FlatbufferMessage(FlatbufferMessage const &Other)
    : DataPtr(std::make_unique<uint8_t[]>(Other.size())),
      DataSize(Other.size()), SourceNameIDHash(Other.SourceNameIDHash),
      Sourcename(Other.Sourcename), ID(Other.ID), Timestamp(Other.Timestamp),
      Valid(Other.Valid) {
  std::memcpy(DataPtr.get(), Other.data(), DataSize);
}

FlatbufferMessage::SrcHash calcSourceHash(std::string const &ID,
                                          std::string const &Name) {
  return std::hash<std::string>{}(ID + Name);
}

void FlatbufferMessage::extractPacketInfo() {
  if (DataSize < 8) {
    Valid = false;
    throw BufferTooSmallError(fmt::format(
        "Flatbuffer was only {} bytes. Expected ≥ 8 bytes.", DataSize));
  }
  std::string FlatbufferID(reinterpret_cast<char const *>(data()) + 4, 4);
  try {
    auto &Reader = FlatbufferReaderRegistry::find(FlatbufferID);
    if (!Reader->verify(*this)) {
      throw NotValidFlatbuffer(fmt::format(
          R"(Buffer which has flatbuffer ID "{}" is not a valid flatbuffer of this type.)",
          FlatbufferID));
    }
    Sourcename = Reader->source_name(*this);
    Timestamp = Reader->timestamp(*this);
    if (Timestamp == 0) {
      throw InvalidFlatbufferTimestamp("Flatbuffer timestamp is zero.");
    }
    SourceNameIDHash = calcSourceHash(FlatbufferID, Sourcename);
    ID = FlatbufferID;
  } catch (std::out_of_range &E) {
    Valid = false;
    throw UnknownFlatbufferID(fmt::format(
        R"(Unable to locate reader with the ID "{}" in the registry.)",
        FlatbufferID));
  }
  Valid = true;
}
} // namespace FileWriter
