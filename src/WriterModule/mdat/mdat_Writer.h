// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2023 European Spallation Source ERIC */

/// \file
///
/// \brief This module contains instructions to write Module DATa about the file
/// via the filewriter, such as the start_time and end_time. The structure is
/// based on the existing WriterModules.
///
/// The class requires the FileWriter::FlatbufferReader interface.
/// The virtual functions to be overridden are called to verify the
/// flatbuffer contents and extract a source name and a timestamp from the
/// flatbuffer.
///
/// The second class which you must implement is a class that inherits from the
/// abstract class WriterModule::Base. This class should implement the
/// actual writing of flatbuffer data to the HDF5 file. More information on the
/// pure virtual functions that you must implement can be found below.
///
/// The call order is as follows:
/// \li WriterModule::Base::Base(..)
/// \li WriterModule::Base::init_hdf(..) or \li WriterModule::Base::reopen(..)
/// \li (multiple) WriterModule::Base::write(..)
/// \li WriterModule::Base::close()
/// \li WriterModule::Base::~Base()

#pragma once

#include "FlatbufferMessage.h"
#include "NeXusDataset/NeXusDataset.h"
#include "WriterModuleBase.h"
#include <iostream>

/// \brief Separate namespace for each module avoids method collisions
namespace WriterModule::mdat {
/// \brief Implements the actual file writing code of the metadata file writing
/// module.
///
/// This class is instantiated twice for every new data source via
///  init_hdf and reopen, where the first of these creates
///  the datasets and the second enables them for writing
class mdat_Writer : public WriterModule::Base {
public:
  /// \brief Constructor should take NXClass "Nxlog" because???
  mdat_Writer()
      : WriterModule::Base(false /*AcceptRepeatedTimestamps*/,
                           "NXlog" /*NXClass*/) {
    std::cout << "mdat module initialised\n";
  }

  /// \brief Close relevant datasets (if any) here.
  /// Note: WriterModule classes are instantiated twice
  /// and WriterModule::Base::close() is called only on the first instantiation.
  /// Any expansion to mdat should ensure those resources are destroyed here.
  ~mdat_Writer() override {}

  /// \brief Initialise datasets and attributes in the HDF5 file;
  /// currently only time (for start_time and end_time).
  /// This must be implemented for HDF5 single writer multiple reader (SWMR)
  /// support.
  WriterModule::InitResult init_hdf(hdf5::node::Group &HDFGroup) const override;

  /// \brief Re-open datasets created when calling
  /// WriterModule::Base::init_hdf(), i.e. on the second instantiation of this.
  /// You cannot do any of the included:
  /// https://support.hdfgroup.org/HDF5/docNewFeatures/SWMR/HDF5_SWMR_Users_Guide.pdf.
  /// This member function is called in the second instantiation of this class
  /// (for a specific data source).
  WriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override; /* {
     NeXusDataset::Mode ndmode = NeXusDataset::Mode::Open;
     std::cout << "mdat_Writer::reopen()\n";
     return init_or_reopen(ndmode, HDFGroup);  //  not monitored?
   }*/

  /// \brief Here we do the data writing.
  /// This member function is called on the second instance of this class.
  /// \note !!Exceptions here lead to an undefined state, avoid throwing them in
  /// this method!! \param Message The structure containing a pointer to a
  /// buffer containing data received from the Kafka broker and the size of the
  /// buffer.
  void
  write(FileWriter::FlatbufferMessage const & /*Message*/) override { /*pass*/
  }

  /// \brief A soecial writing class for metadata
  /// This member function is functional equivalent to the normal
  /// write(FileWriter::FlatbufferMessage const&) method but allows for a call
  /// without requiring a message to be constructed, necessary for the overriden
  /// method which is inherited
  template <typename T> void writemetadata(std::string const &name, T data);

protected:
  // new datasets go here
  NeXusDataset::DateTime mdatStart_datetime;
  NeXusDataset::DateTime mdatEnd_datetime;
  JsonConfig::Field<size_t> ChunkSize{this, "chunk_size", 1024};
  JsonConfig::Field<size_t> StringSize{this, "string_size", 32};
};
} // namespace WriterModule::mdat
  // clang-format on
