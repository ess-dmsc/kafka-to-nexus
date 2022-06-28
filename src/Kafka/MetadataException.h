// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <exception>
#include <string>

/// \brief Class for representing Kafka metadata errors.
///
/// This class was created so that we can more easily determine if we have a
/// metadata error as we want to handle those kinds of errors/exceptions
/// differently to most others.
class MetadataException : public std::exception {
public:
  explicit MetadataException(std::string const &Message) : Message(Message) {}
  const char *what() const noexcept override { return Message.c_str(); }

private:
  std::string Message;
};
