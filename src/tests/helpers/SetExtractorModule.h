#pragma once

#include "FlatbufferReader.h"
#include <memory>
#include <string>

template <class ExtractorType> void setExtractorModule(std::string FbId) {
  auto &Readers = FileWriter::FlatbufferReaderRegistry::getReaders();
  Readers.clear();
  FileWriter::FlatbufferReaderRegistry::Registrar<ExtractorType> RegisterIt(
      FbId);
}