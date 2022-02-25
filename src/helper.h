// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <chrono>
#include <string>
#include <vector>

// \brief Get the current process ID.
// \return Process ID.
int getPID();

// \brief Get the hostname.
// \return The hostname.
std::string getHostName();

// \brief Get a hex string.
// \return A string with characters in the range 0-f. No prefix or suffix.
std::string randomHexString(size_t Length);

// \brief Get the fully qualified domain name.
//
// \return The fully qualified domain name if possible or the hostname if this
// was not possible.
std::string getFQDN();
