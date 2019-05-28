#include "TemplateWriter.h"
#include "ns11_typed_cache_entry_generated.h"

namespace TemplateWriter {

// Instantiates a ReaderClass used for extracting source names, timestamps and
// verifying a flatbuffers.
static FileWriter::FlatbufferReaderRegistry::Registrar<ReaderClass>
    RegisterReader("ns11");

// Creates a factory function used to instantiate zero or more WriterClass, i.e.
// one for every data source which produces data with the file id "test".
static FileWriter::HDFWriterModuleRegistry::Registrar<WriterClass>
    RegisterWriter("ns11");

} // namespace TemplateWriter
