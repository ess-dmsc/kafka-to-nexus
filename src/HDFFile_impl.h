#pragma once
#include <hdf5.h>

class T_HDFFile;

namespace BrightnESS {
namespace FileWriter {

class HDFFile_impl {
  hid_t h5file = -1;
  friend class HDFFile;
  friend class ::T_HDFFile;
};

} // namespace FileWriter
} // namespace BrightnESS
