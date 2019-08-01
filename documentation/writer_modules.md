## Writer Modules

Writer modules for the various FlatBuffer schemas give the file-writer the
ability to parse the FlatBuffers and write them to HDF5.

The actual parsing of the different FlatBuffer schemas and conversion to HDF5 is
handled by modules which register themselves via the `FlatbufferReaderRegistry`
and `HDFWriterModuleRegistry`.  For an example, please search for `Registrar` in
`src/schemas/hs00/`.  Support for new schemas can be added in the same way.


### Module for f142 LogData

[Documentation](writer_module_f142_log_data.md).


### Module for hs00 EventHistogram

[Documentation](writer_module_hs00_event_histogram.md).
