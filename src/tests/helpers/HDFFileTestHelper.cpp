// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDFFileTestHelper.h"

namespace HDFFileTestHelper {

/// Create an in-memory HDF file to use during unit tests
/// \param Filename Name of the file
/// \param OnDisk Can set this to true to write to disk instead, can be useful
/// whilst debugging
/// \return The file object
FileWriter::HDFFile createInMemoryTestFile(const std::string &Filename,
                                           bool OnDisk) {
  hdf5::property::FileAccessList Fapl;
  if (!OnDisk) {
    Fapl.driver(hdf5::file::MemoryDriver());
  }

  FileWriter::HDFFile TestFile;
  TestFile.H5File =
      hdf5::file::create(Filename, hdf5::file::AccessFlags::TRUNCATE,
                         hdf5::property::FileCreationList(), Fapl);

  return TestFile;
}
} // namespace HDFFileTestHelper
