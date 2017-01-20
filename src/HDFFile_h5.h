#pragma once
#include <H5Cpp.h>

namespace BrightnESS {
namespace FileWriter {

/**
Details about the underlying file.
*/
class HDFFile_h5 {
public:
HDFFile_h5(H5::H5File * h5file);
H5::H5File & h5file();
H5::H5File * _h5file;
};

}
}
