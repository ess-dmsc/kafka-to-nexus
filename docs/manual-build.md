# Manual Build

If you have dependencies in non-standard locations:
Locations of dependencies can be supplied via the standard
`CMAKE_INCLUDE_PATH` and `CMAKE_LIBRARY_PATH` variables.

- `flatbuffers` Headers plus `flatc`, therefore set `CMAKE_INCLUDE_PATH` and
  `CMAKE_PROGRAM_PATH`.

- `HDF5`

- `graylog_logger` Additionally, set `USE_GRAYLOG_LOGGER=1`
  - cmake will report if it is found

- `libfmt` Header/Source-only
  - we expect `fmt/[format.cc, format.h]`

- `Google Test` (optional) Easiest way: `git clone https://github.com/google/googletest.git`
  in parallel to this repository, or give the repository location in
  `CMAKE_INCLUDE_PATH` or in `GOOGLETEST_REPOSITORY_DIR`.
  Enable gtest usage by `REQUIRE_GTEST=1`

If you like full fine-grained control over the locations, you can of course set
the locations directly as the package-specific variables which can be looked up
in the `Find...` scripts under `./cmake/` for each package.
