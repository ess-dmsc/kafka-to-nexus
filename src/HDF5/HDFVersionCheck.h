// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <string>

/// \brief Check that the HDF5 version is acceptable.
///
/// Compare the version of the HDF5 headers which the kafka-to-nexus was
/// compiled with against the version of the HDF5 libraries that the
/// kafka-to-nexus is linked against at runtime. Currently, a mismatch in the
/// release number is logged but does not cause panic.
/// \return true if HDF5 version is ok. False otherwise.
bool versionOfHDF5IsOk();

/// \brief A human readable version string of the currently used HDF5 library.
std::string h5VersionStringLinked();
