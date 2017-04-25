#pragma once
#include <hdf5.h>

namespace BrightnESS {
namespace FileWriter {

/**
Details about the underlying file.
*/
class HDFFile_h5 {
public:
  HDFFile_h5(hid_t h5file);
  hid_t h5file();
  hid_t _h5file;
};

} // namespace FileWriter
} // namespace BrightnESS
