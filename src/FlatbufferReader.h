// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FlatbufferMessage.h"
#include "flatbuffers/flatbuffers.h"
#include "logger.h"
#include <array>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace FileWriter {

/// \brief Interface for reading essential information from the flatbuffer which
/// is needed for example to extract timing information and name of the source.
///
/// Example: Please see `src/schemas/ev42/ev42_rw.cpp`.
class FlatbufferReader {
public:
  virtual ~FlatbufferReader() = default;
  using ptr = std::unique_ptr<FlatbufferReader>;

  /// \brief Run the flatbuffer verification and return the result.
  ///
  /// \param Message The flatbuffer message to verify.
  /// \return Returns true if Message is a valid flatbuffer. False otherwise.
  virtual bool verify(FlatbufferMessage const &Message) const = 0;

  /// \brief Extract the 'source name' from a flatbuffer message.
  ///
  /// \param Message The flatbuffer from which the source name should be
  /// extracted. \return The source name of the flatbuffer.
  virtual std::string source_name(FlatbufferMessage const &Message) const = 0;

  /// \brief Extract the timestamp from a flatbuffer.
  ///
  /// \param Message The message from which the timestamp should be extracted.
  /// \return The timestamp of the flatbuffer message.
  virtual uint64_t timestamp(FlatbufferMessage const &Message) const = 0;
};

class FlatBufferSignedIntegersReader : public FlatbufferReader {
  /// \brief Extract the timestamp from a flatbuffer.
  ///
  /// \param Message The message from which the timestamp should be extracted.
  /// \return The timestamp of the flatbuffer message.
public:
  virtual const flatbuffers::Vector<int64_t> *
  timestamp_signed(FlatbufferMessage const &Message) const = 0;
};

/// \brief Keeps track of the registered FlatbufferReader instances.
///
/// See for example `src/schemas/ev42/ev42_rw.cpp` and search for
/// FlatbufferReaderRegistry.
namespace FlatbufferReaderRegistry {
using ReaderPtr = FlatbufferReader::ptr;
std::map<std::string, ReaderPtr> &getReaders();

/// \brief Find a flatbuffer reader instance based on the flatbuffer identifier.
///
/// \param Key The 4 character flatbuffer identifier.
/// \return A pointer to the corresponding FlatbufferReader.
/// \throws std::out_of_range If the/identifier is not found.
FlatbufferReader::ptr &find(std::string const &Key);

/// \brief Add a new flatbuffer reader/extractor.
///
/// \param FlatbufferID The flatbuffer-id that is to be tied to the extractor to
/// be added. \param Item The flatbuffer reader/extractor to be added.
void addReader(std::string const &FlatbufferID, FlatbufferReader::ptr &&Item);

/// \brief A class for facilitating the static registration of flabuffer
/// readers/extractors.
///
/// \tparam T Must be a class that inherits from FileWriter::FlatbufferReader.
template <typename T> class Registrar {
public:
  /// \brief Constructor.
  ///
  /// \param FlatbufferID The flatbuffer-id that is to be tied to the extractor
  /// to be added.
  explicit Registrar(std::string FlatbufferID) {
    FlatbufferReaderRegistry::addReader(FlatbufferID, std::make_unique<T>());
  }
};
} // namespace FlatbufferReaderRegistry
} // namespace FileWriter
