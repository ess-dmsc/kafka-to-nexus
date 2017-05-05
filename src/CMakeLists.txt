# Relative to build root
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ..)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ..)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ..)

if (NOT CMAKE_BUILD_TYPE)
set(CMAKE_BUILD_TYPE Debug)
endif()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

find_package(Fmt REQUIRED)
find_package(Rapidjson REQUIRED)
find_package(RdKafka REQUIRED)
find_package(HDF5_SIMPLE REQUIRED)  # the built in FindHDF5 does not handle 1.10 correctly
find_package(FlatBuffers REQUIRED)
find_package(StreamingDataTypes COMPONENTS d2247ffd906ebeeb68c9b8a2ea9f1d36c1d88a46)
find_package(PCREX REQUIRED)
find_package(GraylogLogger)
find_package(GitCommitExtract)



set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -fPIC -g -D_GLIBCXX_USE_NANOSLEEP")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11 -O0 -fno-inline -ggdb -D_GLIBCXX_USE_NANOSLEEP")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++11 -O3 -D_GLIBCXX_USE_NANOSLEEP")

if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.9.3")
else ()
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=auto")
endif()



set(path_include_common
${FMT_INCLUDE_DIR}
${RDKAFKA_INCLUDE_DIR}
${HDF5_INCLUDE_DIRS}
${FLATBUFFERS_INCLUDE_DIR}
${RAPIDJSON_INCLUDE_DIR}
${CURL_INCLUDE_DIRS}
${PCREX_INCLUDE_DIRS}
${PROJECT_SOURCE_DIR}/src
${PROJECT_BINARY_DIR}/src
)

set(libraries_common
${RDKAFKA_LIBRARIES}
${HDF5_C_LIBRARIES}
${HDF5_CXX_LIBRARIES}
${CURL_LIBRARIES}
${PCREX_LIBRARIES}
pthread
)

set(compile_defs_common "${PCREX_COMPILER_DEFINITIONS}")
list(APPEND compile_defs_common "HAS_REMOTE_API=0")

if (PCREX_MAJOR EQUAL 2)
	list(APPEND compile_defs_common "use_pcre2=1")
endif()

if (${GRAYLOGLOGGER_FOUND} AND ${USE_GRAYLOG_LOGGER})
	message(STATUS "Using graylog_logger")
	list(APPEND compile_defs_common "HAVE_GRAYLOG_LOGGER=1")
	list(APPEND path_include_common ${GRAYLOGLOGGER_INCLUDE_DIR})
	list(APPEND libraries_common libgraylog_logger)
elseif (NOT GRAYLOGLOGGER_FOUND AND USE_GRAYLOG_LOGGER)
	message(WARNING "graylog_logger requested but not found")
endif()


set(tgt "kafka_to_nexus__objects")
add_library(${tgt} OBJECT
Streamer.cxx
StreamMaster.cxx
kafka_util.cxx
Master.cxx
CommandListener.cxx
CommandHandler.cxx
TimeDifferenceFromMessage.cxx
FileWriterTask.cxx
Source.cxx
DemuxTopic.cxx
HDFFile.cxx
SchemaRegistry.cxx
KafkaW.cxx
helper.cxx
logger.cxx
json.cxx
uri.cxx
h5.cxx
date/tz.cpp
date/tzblobs.cpp
schemas/f141/f141_rw.cxx
schemas/f141/synth.cxx
schemas/f142/f142_rw.cxx
schemas/f142/f142_synth.cxx
schemas/f143/f143_rw.cxx
schemas/ev42/ev42_rw.cxx
schemas/ev42/ev42_synth.cxx
schemas/events/schema_amo0.cxx
MainOpt.cxx
${FMT_SRC}
${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx
)
add_dependencies(${tgt} flatbuffers_generate)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common})



set(tgt "send-command")
set(sources
send-command.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} git_commit_current)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common})
target_link_libraries(${tgt} ${libraries_common})



set(tgt "kafka-to-nexus")
set(sources
kafka-to-nexus.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} flatbuffers_generate)
add_dependencies(${tgt} git_commit_current)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common})
target_link_libraries(${tgt} ${libraries_common})

if (have_gtest)
add_gtest_to_target(${tgt})
endif()



set(tgt "dummy")
set(sources
dummy.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} flatbuffers_generate)
add_dependencies(${tgt} git_commit_current)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common})
target_link_libraries(${tgt} ${libraries_common})



if (have_gtest)
add_subdirectory(tests)
endif()
