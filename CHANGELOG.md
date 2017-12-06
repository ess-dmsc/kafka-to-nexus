Breaking changes are tagged by `[breaking]`.

## 2017-11-29

- Rename `FlatbufferReader::sourcename` to `FlatbufferReader::source_name`
  after discussion.

## 2017-11-06

- `[breaking]` `FBSchemaReader` is superceded by `FlatbufferReader`.
- `[breaking]` `FBSchemaWriter` is superceded by `HDFWriterModule`.
- `[breaking]` `SchemaRegistry` is superceded by `FlatbufferReaderRegistry` and
  `HDFWriterModuleRegistry`.
- `[breaking]` The JSON command format has changed, see `README.md`.

## 2017-08-07 e5fdb17

- `[breaking]` Change option `broker-command` to `command-uri` to be better in
line with the new `status-uri`.


## 2017-08-04 fae8ae6

- `[breaking]` Switch from `PCRE` to `std::regex`. This requires at least GCC
4.9.
