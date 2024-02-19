// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDFVersionCheck.h"
#include "logger.h"
#include <H5Ipublic.h>
#include <fmt/format.h>
#include <string>

/// Human readable version of the HDF5 headers that we compile against.
std::string H5VersionStringHeadersCompileTime() {
  return fmt::format("{}.{}.{}", H5_VERS_MAJOR, H5_VERS_MINOR, H5_VERS_RELEASE);
}

std::string h5VersionStringLinked() {
  unsigned h5_vers_major{}, h5_vers_minor{}, h5_vers_release{};
  H5get_libversion(&h5_vers_major, &h5_vers_minor, &h5_vers_release);
  return fmt::format("{}.{}.{}", h5_vers_major, h5_vers_minor, h5_vers_release);
}

bool versionOfHDF5IsOk() {
  unsigned h5_vers_major{}, h5_vers_minor{}, h5_vers_release{};
  H5get_libversion(&h5_vers_major, &h5_vers_minor, &h5_vers_release);
  if (h5_vers_major != H5_VERS_MAJOR) {
    LOG_CRITICAL("HDF5 version mismatch.  compile time: {}  runtime: {}",
                 H5VersionStringHeadersCompileTime(), h5VersionStringLinked());
    return false;
  }
  if (h5_vers_minor != H5_VERS_MINOR) {
    LOG_CRITICAL("HDF5 version mismatch.  compile time: {}  runtime: {}",
                 H5VersionStringHeadersCompileTime(), h5VersionStringLinked());
    return false;
  }
  if (h5_vers_release != H5_VERS_RELEASE) {
    LOG_ERROR("HDF5 version mismatch.  compile time: {}  runtime: {}",
              H5VersionStringHeadersCompileTime(), h5VersionStringLinked());
  }
  return true;
}
