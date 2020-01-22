// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <fmt/format.h>
#include <functional>
#include <h5cpp/hdf5.hpp>
#include <map>
#include <memory>
#include <string>
#include "FlatbufferMessage.h"

namespace Module {


class FlatbufferMessage;
enum class InitResult { ERROR = -1, OK = 0 };

/// \brief Writes a given flatbuffer to HDF.
///
/// Base class for the writer modules which are responsible for actually
/// writing a flatbuffer message to the HDF file.  A HDFWriterModule is
/// instantiated for each 'stream' which is configured in a file writer json
/// command.  The HDFWriterModule class registers itself via a string id which
/// must be unique.  This id is used in the file writer json command.  The id
/// can be arbitrary but should as a convention contain the flatbuffer schema
/// id (`FBID`) like `FBID_<writer-module-name>`.
/// Example: Please see `src/schemas/ev42/ev42_rw.cpp`.
class WriterBase {
public:
  virtual ~WriterBase() = default;

  /// \brief Parses the configuration of a stream.
  ///
  /// \param config_stream Configuration from the write file command for this
  /// stream.
  virtual void parse_config(std::string const &ConfigurationStream) = 0;

  /// \brief Initialise the HDF file.
  ///
  /// Called before any data has arrived with the json configuration of this
  /// stream to allow the `HDFWriterModule` to create any structures in the HDF
  /// file.
  ///
  /// \param HDFGroup The \p HDFGroup into which this HDFWriterModule
  /// should write its data.
  /// \param HDFAttributes Additional attributes as defined in the Nexus
  /// structure which the HDFWriterModule should write to the file. Because the
  /// HDFWriterModule is free to create the structure and data sets according to
  /// its needs, it must also take the responsibility to write these
  /// attributes.
  /// \param HDFAttributes Json string of the attributes associated with the
  /// stream, as defined by the "attributes" key in the Nexus structure.
  ///
  /// \return The result.
  virtual InitResult init_hdf(hdf5::node::Group &HDFGroup,
                              std::string const &HDFAttributes) = 0;

  /// \brief Reopen the HDF objects which are used by this HDFWriterModule.
  ///
  /// \param InitParameters Contains most importantly the \p HDFGroup into
  /// which this HDFWriterModule should write its data.
  ///
  /// \return The result.
  virtual InitResult reopen(hdf5::node::Group &HDFGroup) = 0;

  /// \brief Process the message in some way, for example write to the HDF file.
  ///
  /// \param msg The message to process
  virtual void write(FileWriter::FlatbufferMessage const &Message) = 0;
};

class WriterException : public std::runtime_error {
public:
  explicit WriterException(const std::string &ErrorMessage)
      : std::runtime_error(ErrorMessage) {}
};

using ptr = std::unique_ptr<WriterBase>;

} // namespace Module
