// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
/// \brief Define classes required to implement the ADC file writing module.

#pragma once

#include "FlatbufferReader.h"

namespace AccessMessageMetadata {
using FlatbufferMessage = FileWriter::FlatbufferMessage;
using FBReaderBase = FileWriter::FlatbufferReader;

/// See parent class for documentation.
class ADAr_Extractor : public FBReaderBase {
public:
  [[nodiscard]] bool verify(FlatbufferMessage const &Message) const override;
  [[nodiscard]] std::string source_name(FlatbufferMessage const &) const override;
  [[nodiscard]] uint64_t timestamp(FlatbufferMessage const &Message) const override;
};
} // namespace AccessMessageMetadata
