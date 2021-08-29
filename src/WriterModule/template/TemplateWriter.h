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
/// \brief This file acts as a template for creating file writing modules.
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
#include "FlatbufferMessage.h"
#include "WriterModuleBase.h"
#include <iostream>

/// \brief A separate namespace for this specific file writing module. Use this
/// to minimize the risk of name collisions.
namespace TemplateWriter {

/// \brief Implements the actual file writing code of this file writing module.
///
/// This class is instantiated twice for every new data source. First for
/// initialising the hdf-file (creating datasets etc.). The second instantiation
/// is used for writing the actual data. More information on this can be found
/// in the documentation for the member functions below.
class WriterClass : public WriterModule::Base {
public:
  /// \brief Constructor, initialise state here.
  ///
  /// \note Constructor is executed in a catch-all block (which re-throws)
  /// relatively high up in call hierarchy. You should for this reason probably
  /// try to avoid throwing exceptions here unless you encounter an
  /// unrecoverable state as any exceptions will cause the thread to exit.
  WriterClass() : WriterModule::Base(false, "NXlog") { std::cout << "WriterClass::WriterClass()\n"; }

  /// \brief Use to close datasets and return any other claimed resources.
  ///
  /// Use the destructor to close any open datasets and deallocate buffers etc.
  /// As mentioned previously, this class is instantiated twice for every data
  /// source. WriterModule::Base::close() is called only on the first
  /// instantiation. Thus if you have any resources that you allocate/claim in
  /// the constructor, you should probable return those here (the destructor)
  /// instead of in WriterModule::Base::close().
  ~WriterClass() override { std::cout << "WriterClass::~WriterClass()\n"; }

  /// \brief Used to do additional configuration based on the results of .
  ///
  /// Settings/configurations are passed in JSON form, contained in a
  /// std::string (one is unused, see the parameter documentation).  To extract
  /// the information you want, you will need to implement JSON parsing code
  /// here. As this prototype does not have a return value and exceptions
  /// should not be thrown, the only way to inform the user of a non-fatal
  /// error is to write a log message (see logger.h"). The configurations are
  /// in the base of the JSON object and you should thus be able to extract
  /// relevant settings without navigating a JSON tree, unless the settings are
  /// by design in a tree structure. Examples of extracting settings from the
  /// JSON structure can be found in the files ev42_rw.cpp and f142_rw.cpp.
  ///
  /// \note This call is executed in a catch-all block (which re-throws)
  /// relatively high up in call hierarchy the first time it is called for a
  /// data source. You should for this reason probably try to avoid throwing
  /// exceptions here unless you encounter an unrecoverable state as any
  /// exceptions will cause the thread to exit.
  ///
  /// \param config_stream Contains information about configurations
  /// relevant only to the current instance of this file writing module.
  void config_post_processing() override {
    std::cout << "WriterClass::config_post_processing()\n";
  }

  /// \brief Initialise datasets and attributes in the HDF5 file.
  ///
  /// This member function is used to initialise HDF groups, datasets,
  /// attributes etc. As SWMR (single writer, multiple reader) support in HDF5
  /// only allows for writing data pre-existing datasets, this
  /// function must implement the init functionality. A list of rules to follow
  /// when using SWMR can be found here:
  /// https://support.hdfgroup.org/HDF5/docNewFeatures/SWMR/HDF5_SWMR_Users_Guide.pdf.
  /// When initialising the HDF5 file, the call order (relevant to this class)
  /// is as follows:
  /// \li WriterModule::Base::Base()
  /// \li WriterModule::Base::parse_config()
  /// \li WriterModule::Base::init()
  /// \li WriterModule::Base::close()
  /// \li WriterModule::Base::~Base()
  ///
  /// \note This call is executed in a catch-all block (which re-throws)
  /// relatively high up in call hierarchy the first time it is called for a
  /// data source. You should for this reason probably try to avoid throwing
  /// exceptions here unless you encounter an unrecoverable state as any
  /// exceptions will cause the thread to exit.
  ///
  /// \note The return value of this function is not checked in the current
  /// implementation.
  ///
  /// \param hdf_parent This is the HDF5 group where the relevant
  /// datasets should be created.
  /// \param Tracker Used to register meta data variables.
  /// \return An instance of InitResult. Note that these instances can only be
  /// constructed using the static methods InitResult::OK(),
  /// InitResult::ERROR_IO() and InitResult::ERROR_INCOMPLETE_CONFIGURATION().
  /// Note that the return value is not actually checked and thus returning an
  /// error has no side effects.
  // cppcheck-suppress functionStatic
  WriterModule::InitResult init(hdf5::node::Group &/*HDFGroup*/, MetaData::TrackerPtr /*Tracker*/) override {
    std::cout << "WriterClass::init()\n";
    return WriterModule::InitResult::OK;
  }

  /// \brief Re-open datasets that have been created when calling
  /// WriterModule::Base::init().
  ///
  /// This function should not modify attributes, create datasets or a number of
  /// other things. See the following link for a list of things that you are not
  /// allowed to do when a file has been opened in SWMR mode:
  /// https://support.hdfgroup.org/HDF5/docNewFeatures/SWMR/HDF5_SWMR_Users_Guide.pdf.
  /// This member function is called in the second instantiation of this class
  /// (for a specific data source). The order in which methods (relevant to this
  /// class) are called is as follows:
  /// \li WriterModule::Base::Base()
  /// \li WriterModule::Base::parse_config()
  /// \li WriterModule::Base::reopen()
  /// \li Multiple calls to WriterModule::Base::write()
  /// \li WriterModule::Base::~Base()
  ///
  /// \note This call is executed in a catch-all block (which re-throws)
  /// relatively high up in call hierarchy the first time it is called for a
  /// data source. You should for this reason probably try to avoid throwing
  /// exceptions here unless you encounter an unrecoverable state as any
  /// exceptions will cause the thread to exit.
  ///
  /// \param hdf_parent This is HDF5 group which has the datasets created
  /// using the call to init().
  ///
  /// \return An instance of InitResult. Note that these instances can only be
  /// constructed using the static methods InitResult::OK(),
  /// InitResult::ERROR_IO() and InitResult::ERROR_INCOMPLETE_CONFIGURATION().
  // cppcheck-suppress functionStatic
  WriterModule::InitResult reopen(hdf5::node::Group &/*HDFGroup*/) override {
    std::cout << "WriterClass::reopen()\n";
    return WriterModule::InitResult::OK;
  }

  /// \brief Implements the data writing functionality of the file writing
  /// module.
  ///
  /// To properly support SWMR, only writes to datasets are allowed in this
  /// member function. See the following link for limitations when using a file
  /// in SWMR mode:
  /// https://support.hdfgroup.org/HDF5/docNewFeatures/SWMR/HDF5_SWMR_Users_Guide.pdf.
  /// This member function is called on the second instance of this class for a
  /// specific data source. The order in which methods (relevant to this class)
  /// are called is as follows:
  /// \li WriterModule::Base::Base()
  /// \li WriterModule::Base::parse_config()
  /// \li WriterModule::Base::reopen()
  /// \li Multiple calls to WriterModule::Base::write()
  /// \li WriterModule::Base::~Base()
  ///
  /// \note Try to avoid throwing any exceptions here as it (appears) to likely
  /// either crash the application or leave it in an inconsistent state.
  ///
  /// \param Message The structure containing a pointer to a buffer
  /// containing data received from the Kafka broker and the size of the buffer.
  // cppcheck-suppress functionStatic
  void write(FileWriter::FlatbufferMessage const &/*Message*/) override {
    std::cout << "WriterClass::write()\n";
  }
};
} // namespace TemplateWriter
// clang-format on
