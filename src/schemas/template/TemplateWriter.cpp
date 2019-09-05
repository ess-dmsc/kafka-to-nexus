// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "TemplateWriter.h"

namespace TemplateWriter {

// Instantiates a ReaderClass used for extracting source names, timestamps and
// verifying a flatbuffers.
static FileWriter::FlatbufferReaderRegistry::Registrar<ReaderClass>
    RegisterReader("test");

// Creates a factory function used to instantiate zero or more WriterClass, i.e.
// one for every data source which produces data with the file id "test".
static FileWriter::HDFWriterModuleRegistry::Registrar<WriterClass>
    RegisterWriter("test");

} // namespace TemplateWriter
