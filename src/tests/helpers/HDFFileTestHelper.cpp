#include "HDFFileTestHelper.h"

namespace HDFFileTestHelper {

FileWriter::HDFFile createInMemoryTestFile(const std::string &Filename) {
  hdf5::property::FileAccessList fapl;
  fapl.driver(hdf5::file::MemoryDriver());

  FileWriter::HDFFile TestFile;
  TestFile.H5File =
      hdf5::file::create(Filename, hdf5::file::AccessFlags::TRUNCATE,
                         hdf5::property::FileCreationList(), fapl);

  return TestFile;
}
} // namespace HDFFileTestHelper
