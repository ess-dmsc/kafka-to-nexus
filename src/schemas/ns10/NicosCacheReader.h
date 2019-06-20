/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
///
/// \brief Writing module for the NICOS cache values.

#pragma once
#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include "../../h5.h"

namespace FileWriter {
namespace Schemas {
namespace ns10 {

class CacheReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FlatbufferMessage const &Message) const override;

  std::string
  source_name(FlatbufferMessage const &Message) const override;

  uint64_t
  timestamp(FlatbufferMessage const &Message) const override;

private:
};

} // namespace ns10
} // namespace Schemas
} // namespace FileWriter
