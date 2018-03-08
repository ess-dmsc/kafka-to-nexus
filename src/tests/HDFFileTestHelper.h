#pragma once

#include "../HDFFile.h"
#include <h5cpp/hdf5.hpp>

namespace HDFFileTestHelper {

FileWriter::HDFFile createInMemoryTestFile(const std::string &Filename);
}
