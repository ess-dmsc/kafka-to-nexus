// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Msg.h"
#include "logger.h"

namespace FileWriter {
class FlatbufferError : public std::runtime_error {
public:
  explicit FlatbufferError(std::string const &What)
      : std::runtime_error(What) {}
};

class BufferTooSmallError : public FlatbufferError {
public:
  explicit BufferTooSmallError(const std::string &what)
      : FlatbufferError(what) {};
};

class UnknownFlatbufferID : public FlatbufferError {
public:
  explicit UnknownFlatbufferID(const std::string &what)
      : FlatbufferError(what) {};
};

class InvalidFlatbufferTimestamp : public FlatbufferError {
public:
  explicit InvalidFlatbufferTimestamp(const std::string &what)
      : FlatbufferError(what) {};
};

class NotValidFlatbuffer : public FlatbufferError {
public:
  explicit NotValidFlatbuffer(const std::string &what)
      : FlatbufferError(what) {};
};

/// \brief A wrapper around a databuffer which holds a flatbuffer.
///
/// Used to simplify passing around flatbuffers and the most important pieces of
/// meta-data.
class FlatbufferMessage {
public:
  using SrcHash = size_t;

  /// \brief This constructor is used in the unit testing code to simplify
  /// set-up.
  FlatbufferMessage() = default;

  /// \brief Creates a flatbuffer message, verifies the message and extracts
  /// metadata.
  ///
  /// \note This constructor should be used as little as possible and then only
  /// in unit tests.
  ///
  /// \param BufferPtr Pointer to memory containing the data.
  /// \param Size Number of bytes in message.
  /// \note Will make a copy of the data in the Kafka message.
  FlatbufferMessage(uint8_t const *BufferPtr, size_t Size);

  /// \brief Creates a flatbuffer message, verifies the message and extracts
  /// metadata.
  ///
  /// \param KafkaMessage The Kafka message used to create the Flatbuffer
  /// message.
  /// \note Will make a copy of the data in the Kafka message.
  explicit FlatbufferMessage(FileWriter::Msg const &KafkaMessage);

  /// \brief Creates a flatbuffer message, verifies the message and extracts
  /// metadata. Copy constructor version.
  ///
  /// \param Other The instance of the FlatbufferMessage that is to be copied.
  /// \note Will make a copy of the data in the Kafka message.
  FlatbufferMessage(FlatbufferMessage const &Other);

  /// \\bried Default destructor.
  ~FlatbufferMessage() = default;

  FlatbufferMessage &operator=(FlatbufferMessage const &Other) {
    DataPtr = std::make_unique<uint8_t[]>(Other.DataSize);
    std::memcpy(DataPtr.get(), Other.DataPtr.get(), Other.DataSize);
    DataSize = Other.DataSize;
    SourceNameIDHash = Other.SourceNameIDHash;
    Sourcename = Other.Sourcename;
    ID = Other.ID;
    Timestamp = Other.Timestamp;
    Valid = Other.Valid;
    return *this;
  }

  /// \brief Returns the state of the FlatbufferMessage.
  ///
  /// \return `true` if valid, `false` if not.
  bool isValid() const { return Valid; };

  /// \brief Get the source name of the flatbuffer.
  ///
  /// Extracted using FileWriter::FlatbufferReader::source_name().
  ///
  /// \return The source name if flatbuffer is valid, an empty string if it is
  /// not.
  std::string getSourceName() const { return Sourcename; };

  /// \brief Get the timestamp of the flatbuffer.
  ///
  /// Extracted using FileWriter::FlatbufferReader::timestamp().
  ///
  /// \return The timestamp if flatbuffer is valid, 0 if it is not.
  auto getTimestamp() const { return Timestamp; };

  /// \brief Get the hash from a combination of the flatbuffer type and source
  /// name.
  ///
  /// \return The std::hash<std::string> from flatbuffer id + source name.
  /// Returns 0 if flatbuffer is invalid.
  SrcHash getSourceHash() const { return SourceNameIDHash; };

  /// \brief Get flatbuffer ID.
  ///
  /// \return Returns the four character flatbuffer ID or empty string if
  /// invalid.
  std::string getFlatbufferID() const { return ID; };

  /// \brief Get pointer to flatbuffer.
  ///
  /// \return Pointer to flatbuffer data if flatbuffer is valid, `nullptr` if it
  /// is not.
  uint8_t const *data() const { return DataPtr.get(); };

  /// \brief Get size of flatbuffer.
  ///
  /// \return Size of flatbuffer in bytes if flatbuffer is valid, 0 if it is
  /// not.
  size_t size() const { return DataSize; };

private:
  void extractPacketInfo();
  std::unique_ptr<uint8_t[]> DataPtr;
  size_t DataSize{0};
  SrcHash SourceNameIDHash{0};
  std::string Sourcename;
  std::string ID;
  std::int64_t Timestamp{0};
  bool Valid{false};
};

FlatbufferMessage::SrcHash calcSourceHash(std::string const &ID,
                                          std::string const &Name);
} // namespace FileWriter
