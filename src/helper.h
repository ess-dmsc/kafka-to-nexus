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
#include <vector>
#include <chrono>

int getpid_wrapper();

std::string gethostname_wrapper();

std::vector<char> readFileIntoVector(std::string const &FileName);

std::chrono::duration<long long int, std::milli> getCurrentTimeStampMS();
