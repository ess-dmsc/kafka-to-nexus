set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../tests)


set(tgt "tests")
set(sources
tests.cxx
roundtrip.cxx
HDFFile.cxx
helper.cxx
json.cxx
uri.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} flatbuffers_generate)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common})
target_link_libraries(${tgt} ${libraries_common})
add_gtest_to_target(${tgt})


set(tgt "streamer_test")
set(sources
streamer_test.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} flatbuffers_generate)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common})
target_link_libraries(${tgt} ${libraries_common})
add_gtest_to_target(${tgt})


set(tgt "streammaster_test")
set(sources
streammaster_test.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} flatbuffers_generate)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common})
target_link_libraries(${tgt} ${libraries_common})
add_gtest_to_target(${tgt})


configure_file(msg-conf-new-01.json ../../tests/msg-conf-new-01.json)
configure_file(msg-conf-new-02.json ../../tests/msg-conf-new-02.json)
configure_file(msg-conf-new-03.json ../../tests/msg-conf-new-03.json)
configure_file(msg-conf-new-04.json ../../tests/msg-conf-new-04.json)
configure_file(msg-cmd-new-03.json ../../tests/msg-cmd-new-03.json)
configure_file(schema-command.json ../../tests/schema-command.json)
