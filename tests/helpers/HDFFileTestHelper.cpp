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
std::unique_ptr<DebugHDFFile>
createInMemoryTestFile(const std::string &Filename, bool OnDisk) {
  if (OnDisk) {
    return std::make_unique<DiskHDFFile>(Filename);
  }
  return std::make_unique<InMemoryHDFFile>();
}

InMemoryHDFFile::InMemoryHDFFile() {
  hdf5::property::FileAccessList Fapl;
  Fapl.driver(hdf5::file::MemoryDriver());
  hdfFile() =
      hdf5::file::create("unused", hdf5::file::AccessFlags::Truncate, {}, Fapl);
}

DiskHDFFile::DiskHDFFile(std::string const &FileName) {
  hdfFile() =
      hdf5::file::create(FileName, hdf5::file::AccessFlags::Truncate, {}, {});
}

} // namespace HDFFileTestHelper
