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
///
/// \brief Writing module for the NICOS cache values.

#pragma once
#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"

namespace FileWriter {
namespace Schemas {
namespace ns10 {

class CacheReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override;

  std::string source_name(FlatbufferMessage const &Message) const override;

  uint64_t timestamp(FlatbufferMessage const &Message) const override;

private:
};

} // namespace ns10
} // namespace Schemas
} // namespace FileWriter
