set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../tests)

find_package(Trompeloeil REQUIRED)

set(UnitTests_SRC
UnitTests.cpp
H5Tests.cpp
HDFFile.cpp
HDFFileAttributesTest.cpp
HDFFileTestHelper.cpp
helper.cpp
json.cpp
uri.cpp
status_test.cpp
StatusWriterTests.cpp
CommandHandlerTests.cpp
stream_master.cpp
FastSampleEnvironmentWriterTests.cpp
NeXusDatasets.cpp
ReaderRegistration.cpp
WriterRegistration.cpp
TemplateWriterTests.cpp
DemuxerTests.cpp
MessageTest.cpp
StreamerTest.cpp
AddReader.cpp
Schema_f142.cpp
EventLogger.cpp
MainOpt.cpp
)
set(UnitTests_INC
HDFFileTestHelper.h
AddReader.h
)

add_executable(UnitTests EXCLUDE_FROM_ALL
  ${UnitTests_SRC}
  ${UnitTests_INC}
  $<TARGET_OBJECTS:kafka_to_nexus__objects>
  ${WRITER_MODULES}
)

add_dependencies(UnitTests flatbuffers_generate)
target_compile_definitions(UnitTests PRIVATE ${compile_defs_common})
target_include_directories(UnitTests PRIVATE ${path_include_common} ${Trompeloeil_INCLUDE_DIR})
target_link_libraries(UnitTests
  ${GTEST_BOTH_LIBRARIES}
  ${libraries_common}
  fmt::fmt
)

get_filename_component(TEST_DATA_PATH "data/" ABSOLUTE)
target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")

enable_testing()
add_test(AllTests UnitTests)
