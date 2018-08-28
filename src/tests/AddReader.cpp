/// Used to get around a namespace collision problem in HDFFile.cpp

#include "AddReader.h"
#include "../schemas/ev42/ev42_rw.h"
#include "../schemas/f142/FlatbufferReader.h"
#include "../schemas/f142/f142_rw.h"

void AddF142Reader() {
  using FileWriter::FlatbufferReaderRegistry::ReaderPtr;
  std::map<std::string, ReaderPtr> &Readers =
      FileWriter::FlatbufferReaderRegistry::getReaders();
  Readers.clear();
  FileWriter::FlatbufferReaderRegistry::Registrar<
      FileWriter::Schemas::f142::FlatbufferReader>
      RegisterIt("f142");
}

void AddEv42Reader() {
  using FileWriter::FlatbufferReaderRegistry::ReaderPtr;
  std::map<std::string, ReaderPtr> &Readers =
      FileWriter::FlatbufferReaderRegistry::getReaders();
  Readers.clear();
  FileWriter::FlatbufferReaderRegistry::Registrar<
      FileWriter::Schemas::ev42::FlatbufferReader>
      RegisterIt("ev42");
}
