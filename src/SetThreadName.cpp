// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "SetThreadName.h"
#include <pthread.h>

void setThreadName(std::string const &NewName) {
#ifdef __APPLE__
  pthread_setname_np(NewName.c_str());
#elif __linux__
  pthread_setname_np(pthread_self(), NewName.c_str());
#else
#pragma message("Unsupported platform. Unable to set thread name.")
#endif
}