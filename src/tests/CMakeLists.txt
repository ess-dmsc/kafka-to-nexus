set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../tests)


set(tgt "tests")
set(sources
tests.cxx
roundtrip.cxx
HDFFile.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} flatbuffers_generate)
target_include_directories(${tgt} PRIVATE
${path_include_fmt}
${path_include_hdf5}
${path_include_rdkafka}
${path_include_flatbuffers}
${path_include_rapidjson}
${CMAKE_CURRENT_BINARY_DIR}/..
)
target_link_libraries(${tgt} libhdf5)
target_link_libraries(${tgt} libhdf5cpp)
target_link_libraries(${tgt} librdkafka)
target_link_libraries(${tgt} librdkafka++)
target_link_libraries(${tgt} pcre2-8)
if (have_gtest)
target_compile_definitions(${tgt} PRIVATE HAVE_GTEST=1)
target_include_directories(${tgt} PRIVATE ${path_include_gtest})
target_link_libraries(${tgt} libgtest)
endif()


set(tgt "streamer_test")
set(sources
streamer_test.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} flatbuffers_generate)
target_include_directories(${tgt} PRIVATE
${path_include_fmt}
${path_include_hdf5}
${path_include_rdkafka}
${path_include_flatbuffers}
${path_include_rapidjson}
${CMAKE_CURRENT_BINARY_DIR}
..
)
target_link_libraries(${tgt} libhdf5)
target_link_libraries(${tgt} libhdf5cpp)
target_link_libraries(${tgt} librdkafka)
target_link_libraries(${tgt} librdkafka++)
target_link_libraries(${tgt} pcre2-8)
if (have_gtest)
target_compile_definitions(${tgt} PRIVATE HAVE_GTEST=1)
target_include_directories(${tgt} PRIVATE ${path_include_gtest})
target_link_libraries(${tgt} libgtest)
endif()


set(tgt "streammaster_test")
set(sources
streammaster_test.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} flatbuffers_generate)
target_include_directories(${tgt} PRIVATE
${path_include_fmt}
${path_include_hdf5}
${path_include_rdkafka}
${path_include_flatbuffers}
${path_include_rapidjson}
${CMAKE_CURRENT_BINARY_DIR}
..
)
target_link_libraries(${tgt} libhdf5)
target_link_libraries(${tgt} libhdf5cpp)
target_link_libraries(${tgt} librdkafka)
target_link_libraries(${tgt} librdkafka++)
target_link_libraries(${tgt} pcre2-8)
if (have_gtest)
target_compile_definitions(${tgt} PRIVATE HAVE_GTEST=1)
target_include_directories(${tgt} PRIVATE ${path_include_gtest})
target_link_libraries(${tgt} libgtest)
endif()

configure_file(msg-conf-new-01.json ../../tests/msg-conf-new-01.json)
configure_file(msg-conf-new-02.json ../../tests/msg-conf-new-02.json)
configure_file(msg-conf-new-03.json ../../tests/msg-conf-new-03.json)
configure_file(msg-conf-new-04.json ../../tests/msg-conf-new-04.json)
configure_file(msg-cmd-new-03.json ../../tests/msg-cmd-new-03.json)
configure_file(schema-command.json ../../tests/schema-command.json)
