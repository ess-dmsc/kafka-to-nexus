#pragma once

#include <string>
#include <memory>
#include "FlatbufferReader.h"

template <class ExtractorType>
void setExtractorModule(std::string FbId) {
  auto &Readers =
      FileWriter::FlatbufferReaderRegistry::getReaders();
  Readers.clear();
  FileWriter::FlatbufferReaderRegistry::Registrar<ExtractorType>
      RegisterIt(FbId);
}