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
#include <h5cpp/hdf5.hpp>
#include <memory>
#include <string>
#include <string_view>

namespace WriterModule {

enum class InitResult { ERROR = -1, OK = 0 };

/// \brief Writes a given flatbuffer to HDF.
///
/// Base class for the writer modules which are responsible for actually
/// writing a flatbuffer message to the HDF file.  A writer module is
/// instantiated for each 'stream' which is configured in a file writer json
/// command.  The writer module class registers itself via a string id which
/// must be unique.  This id is used in the file writer json command.  The id
/// can be arbitrary but should as a convention contain the flatbuffer schema
/// id (`FBID`) like `FBID_<writer-module-name>`.
/// Example: Please see `src/schemas/ev42/ev42_rw.cpp`.
class Base {
public:
  Base(bool AcceptRepeatedTimestamps, std::string_view NX_class)
      : WriteRepeatedTimestamps(AcceptRepeatedTimestamps), NX_class(NX_class) {}
  virtual ~Base() = default;

  bool acceptsRepeatedTimestamps() const { return WriteRepeatedTimestamps; }

  auto defaultNeXusClass() const { return NX_class; }

  /// \brief Parses the configuration of a stream.
  ///
  /// \param config_stream Configuration from the write file command for this
  /// stream.
  virtual void parse_config(std::string const &ConfigurationStream) = 0;

  /// \brief Initialise the HDF file.
  ///
  /// Called before any data has arrived with the json configuration of this
  /// stream to allow the writer module to create any structures in the HDF
  /// file.
  ///
  /// \param HDFGroup The \p HDFGroup into which this module
  /// should write its data.
  /// \param HDFAttributes Additional attributes as defined in the Nexus
  /// structure which the module should write to the file. Because the
  /// writer module is free to create the structure and data sets according to
  /// its needs, it must also take the responsibility to write these
  /// attributes.
  /// \param HDFAttributes Json string of the attributes associated with the
  /// stream, as defined by the "attributes" key in the Nexus structure.
  ///
  /// \return The result.
  virtual InitResult init_hdf(hdf5::node::Group &HDFGroup) = 0;

  /// \brief Reopen the HDF objects which are used by this writer module.
  ///
  /// \param InitParameters Contains most importantly the \p HDFGroup into
  /// which this module should write its data.
  ///
  /// \return The result.
  virtual InitResult reopen(hdf5::node::Group &HDFGroup) = 0;

  /// \brief Process the message in some way, for example write to the HDF file.
  ///
  /// \param msg The message to process
  virtual void write(FileWriter::FlatbufferMessage const &Message) = 0;

private:
  bool WriteRepeatedTimestamps;
  std::string_view NX_class;
};

class WriterException : public std::runtime_error {
public:
  explicit WriterException(const std::string &ErrorMessage)
      : std::runtime_error(ErrorMessage) {}
};

using ptr = std::unique_ptr<Base>;

} // namespace WriterModule
