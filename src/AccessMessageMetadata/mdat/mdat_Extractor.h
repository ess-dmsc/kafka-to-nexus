// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
///
/// \brief This file acts as a mdat for creating file writing modules.
///
/// All of the classes required are explained here. The
/// only thing missing from this header file is the registration of the file
/// writing module which must reside in an implementation file. See the
/// accompanying TestWriter.cpp for instructions on how this should be done.
///
/// A file writing module comprises two classes. The first class is the
/// flatbuffer reader which must implement the FileWriter::FlatbufferReader
/// interface. The virtual functions to be overridden are called to verify the
/// flatbuffer contents and extract a source name and a timestamp from the
/// flatbuffer. See the documentation of the individual member functions for
/// more information on how these should be implemented.
/// The second class which you must implement is a class that inherits from the
/// abstract class WriterModule::Base. This class should implement the
/// actual writing of flatbuffer data to the HDF5 file. More information on the
/// pure virtual functions that you must implement can be found below.

// clang-format off
// Formatting disabled because of bug in cppcheck inline suppression.

#pragma once
#include "FlatbufferReader.h"
#include "flatbuffers/flatbuffers.h"
#include <pl72_run_start_generated.h>
//#include "WriterModuleBase.h"
#include <iostream>

/// \brief A separate namespace for this specific file writing module. Use this
/// to minimize the risk of name collisions.
namespace AccessMessageMetadata {

/// \brief This class is used to extract information from a flatbuffer which
/// uses a specific four character file identifier.
///
/// See TestWriter.cpp for code which is used to tie a file identifier to an
/// instance of this class. Note that this class is only instantiated once in
/// the entire run of the application.
class mdat_Extractor : public FileWriter::FlatbufferReader {
public:
  /// \brief Is used to verify the contents of a flatbuffer.
  ///
  /// The `Verify*Buffer()` function available for the flatbuffer schema that
  /// you are using can be called here. This function check all offsets, sizes
  /// of fields, and null termination of strings to ensure that when a buffer
  /// is accessed, all reads will end up inside the buffer. Implementing
  /// additional checks, e.g. determining if a value falls with in an expected
  /// range, is also possible here.
  ///
  /// \note There are at least two good arguments for having this function not
  /// actually implement any verification of the data and return true by
  /// default: \li This member function is called on the data AFTER
  /// FileWriter::FlatbufferReader::source_name(), thus somewhat defeating the
  /// point of having it.
  /// \li Verifying a flatbuffer can (for some flatbuffer schemas) be relatively
  /// expensive and it is currently not possible to set the application to not
  /// call this function.
  ///
  /// The alternative is to do verification in the file writing part of the code
  /// i.e. WriterModule::Base::write().
  ///
  /// \note Try to avoid throwing any exceptions here as it (appears) to likely
  /// either crash the application or leave it in an inconsistent state.
  ///
  /// \param Message The structure containing a pointer to a buffer
  /// containing data received from the Kafka broker and the size of the
  /// buffer.
  ///
  /// \return `true` if the data was verified as "correct", `false` otherwise.
  // cppcheck-suppress functionStatic
  bool verify(FileWriter::FlatbufferMessage const &/*Message*/) const override {
    std::cout << "ReaderClass::verify()\n";
    return true;
  }

  /// \brief Extract the name of the data source from the flatbuffer.
  ///
  /// When setting up the file writer, a point (group) is tied to a specific
  /// module instance using the four character identifier of a file writing
  /// module and the name of the source of the data. This function is used to
  /// extract the name of a source given the relevant flatbuffer. Although the
  /// name can be as simple as a string contained in a flatbuffer, it might be
  /// appropriate in some cases to construct the name from (e.g.) a string and a
  /// channel number.
  ///
  /// \note In the current implementation, this member function might be called
  /// several times on the same flatbuffer.
  /// \note Try to avoid throwing any exceptions here as it (appears) to likely
  /// either crash the application or leave it in an inconsistent state.
  ///
  /// \param Message The structure containing a pointer to a buffer
  /// containing data received from the Kafka broker and the size of the
  /// buffer.
  ///
  /// \return The name of the source of the data in the flatbuffer pointed to by
  /// the Message parameter.
  // cppcheck-suppress functionStatic
  std::string source_name(FileWriter::FlatbufferMessage const &Message) const override {
    return GetRunStart(Message.data())->nexus_structure()->c_str();
  }

  /// \brief Extract the timestamp of a flatbuffer.
  ///
  /// The file writer can be set to write data only in a specific "time window".
  /// Thus function is in that case used to extract the time of a flatbuffer and
  /// thus decide if its data should be written to file or not. Note that there
  /// are currently no specification or documentation as to which epoch is to
  /// be used nor unit of time and prefix. With that said, the current
  /// implementations appears to try to use (when possible) Unix epoch and
  /// nanoseconds.
  ///
  /// \note Try to avoid throwing any exceptions here as it (appears) to likely
  /// either crash the application or leave it in an inconsistent state.
  ///
  /// \param Message The structure containing a pointer to a buffer
  /// containing data received from the Kafka broker and the size of the buffer.
  ///
  /// \return The timestamp of the flatbuffer as nanoseconds since Unix epoch
  /// (see above).
  // cppcheck-suppress functionStatic
  uint64_t timestamp(FileWriter::FlatbufferMessage const &Message) const override {
    uint64_t returnTime = GetRunStart(Message.data())->start_time();
    if( !returnTime )
      returnTime = GetRunStart(Message.data())->stop_time();
    return returnTime;
  }
};

} // namespace mdatWriter
// clang-format on
