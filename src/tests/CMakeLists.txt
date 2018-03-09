set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../tests)

message(STATUS "libraries_testing ${libraries_testing}")

set(tgt "tests")
set(sources
tests.cxx
roundtrip.cxx
HDFFile.cxx
HDFFileAttributesTest.cpp
HDFFileTestHelper.cpp
helper.cxx
json.cxx
uri.cxx
status_test.cxx
status_writer.cxx
command_handler.cxx
stream_master.cxx
streamer_options.cxx
#broker_settings.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
set(includes
HDFFileTestHelper.h
)
add_executable(${tgt} ${sources} ${includes})

set(libraries_testing ${libraries_common})

if(NOT ${FAKE_RDKAFKA} STREQUAL "")
	message(STATUS "Build with fake librdkafka: " ${FAKE_RDKAFKA})
	list(REMOVE_ITEM libraries_testing ${RDKAFKA_LIBRARIES_CXX} ${RDKAFKA_LIBRARIES_C})
	list(APPEND libraries_testing 
		${FAKE_RDKAFKA}/lib/librdkafka-fake.a 
	 	${FAKE_RDKAFKA}/lib/librdkafka-fake-utils.a
		)
	list(APPEND sources
		streamer.cxx
		)
endif()

add_executable(${tgt} ${sources})

add_dependencies(${tgt} flatbuffers_generate)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common} ${FAKE_RDKAFKA})
target_link_libraries(${tgt} ${libraries_testing})
add_gtest_to_target(${tgt})

configure_file(msg-conf-new-01.json ../../tests/msg-conf-new-01.json)
configure_file(msg-conf-new-02.json ../../tests/msg-conf-new-02.json)
configure_file(msg-conf-new-03.json ../../tests/msg-conf-new-03.json)
configure_file(msg-conf-new-04.json ../../tests/msg-conf-new-04.json)
configure_file(msg-cmd-new-00.json ../../tests/msg-cmd-new-00.json)
configure_file(msg-cmd-new-01.json ../../tests/msg-cmd-new-01.json)
configure_file(msg-cmd-new-03.json ../../tests/msg-cmd-new-03.json)
configure_file(schema-command.json ../../tests/schema-command.json)
