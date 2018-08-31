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
status_writer.cpp
CommandHandlerTests.cpp
stream_master.cpp
streamer_options.cpp
MainOptTest.cpp
FastSampleEnvironmentWriterTests.cpp
NeXusDatasets.cpp
TemplateWriterTests.cpp
DemuxerTests.cpp
MessageTest.cpp
StreamerTest.cpp
Schema_f142.cpp
CoverageFix.cpp
)
set(UnitTests_INC
HDFFileTestHelper.h
)

add_executable(UnitTests EXCLUDE_FROM_ALL ${UnitTests_SRC} ${UnitTests_INC})

add_dependencies(UnitTests flatbuffers_generate)
target_compile_definitions(UnitTests PRIVATE ${compile_defs_common})
target_include_directories(UnitTests PRIVATE ${path_include_common} ${Trompeloeil_INCLUDE_DIR})
target_link_libraries(UnitTests kafka_to_nexus__objects ${GTEST_BOTH_LIBRARIES})

get_filename_component(TEST_DATA_PATH "data/" ABSOLUTE)
target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")

enable_testing()
add_test(AllTests UnitTests)
