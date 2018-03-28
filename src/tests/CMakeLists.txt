set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../tests)

set(UnitTests_SRC
tests.cpp
roundtrip.cpp
HDFFile.cpp
HDFFileAttributesTest.cpp
HDFFileTestHelper.cpp
helper.cpp
json.cpp
uri.cpp
status_test.cpp
status_writer.cpp
command_handler.cpp
stream_master.cpp
streamer_options.cpp
MainOpt_tests.cpp
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
set(UnitTests_INC
HDFFileTestHelper.h
roundtrip.h
)

# set(libraries_testing ${libraries_common})
# if(NOT ${FAKE_RDKAFKA} STREQUAL "")
#   message(STATUS "Build with fake librdkafka: " ${FAKE_RDKAFKA})
#   list(REMOVE_ITEM libraries_testing ${RDKAFKA_LIBRARIES_CXX} ${RDKAFKA_LIBRARIES_C})
#   list(APPEND libraries_testing
#     ${FAKE_RDKAFKA}/lib/librdkafka-fake.a
#      ${FAKE_RDKAFKA}/lib/librdkafka-fake-utils.a
#     )
#   list(APPEND sources
#     streamer.cxx
#     )
# endif()

add_executable(UnitTests ${UnitTests_SRC} ${UnitTests_INC})


add_dependencies(UnitTests flatbuffers_generate)
target_compile_definitions(UnitTests PRIVATE ${compile_defs_common})
target_include_directories(UnitTests PRIVATE ${path_include_common} ${FAKE_RDKAFKA})
target_link_libraries(UnitTests ${libraries_testing} ${GTEST_BOTH_LIBRARIES} ${libraries_common})

get_filename_component(TEST_DATA_PATH "data/" ABSOLUTE)
target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")

enable_testing()
add_test(AllTests UnitTests)
