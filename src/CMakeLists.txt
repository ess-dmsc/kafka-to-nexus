# Relative to build root
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

find_package(Fmt REQUIRED)
find_package(Rapidjson REQUIRED)
find_package(RdKafka REQUIRED)
find_package(HDF5_SIMPLE REQUIRED)  # the built in FindHDF5 does not handle 1.10 correctly
find_package(FlatBuffers REQUIRED)
find_package(MPI REQUIRED)
find_package(Jemalloc REQUIRED)
find_package(StreamingDataTypes COMPONENTS d2247ffd906ebeeb68c9b8a2ea9f1d36c1d88a46)
find_package(GraylogLogger)
find_package(GitCommitExtract)


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -fPIC -g -D_GLIBCXX_USE_NANOSLEEP")
if (DEFINED _GLIBCXX_USE_CXX11_ABI)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=${_GLIBCXX_USE_CXX11_ABI}")
endif()

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
    ${MPI_INCLUDE_DIR}
    ${JEMALLOC_INCLUDE_DIR}
    ${FLATBUFFERS_INCLUDE_DIR}
    ${RAPIDJSON_INCLUDE_DIR}
    ${CURL_INCLUDE_DIRS}
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src
)

set(libraries_common
    ${RDKAFKA_LIBRARIES}
    ${HDF5_C_LIBRARIES}
    ${MPI_LIBRARIES}
    ${JEMALLOC_LIBRARIES}
    ${CURL_LIBRARIES}
    pthread
    z
)

set(compile_defs_common "HAS_REMOTE_API=0")

if (${GRAYLOGLOGGER_FOUND} AND ${USE_GRAYLOG_LOGGER})
	message(STATUS "Using graylog_logger")
	list(APPEND compile_defs_common "HAVE_GRAYLOG_LOGGER=1")
	list(APPEND path_include_common ${GRAYLOGLOGGER_INCLUDE_DIR})
	list(APPEND libraries_common libgraylog_logger)
elseif (NOT GRAYLOGLOGGER_FOUND AND USE_GRAYLOG_LOGGER)
	message(WARNING "graylog_logger requested but not found")
endif()

set(kafka_to_nexus_SRC
    Status.cxx
    StatusWriter.cxx
    Errors.cxx
    Streamer.cxx
    kafka_util.cxx
    Master.cxx
    CommandListener.cxx
    CommandHandler.cxx
    TimeDifferenceFromMessage.cxx
    FileWriterTask.cxx
    Source.cxx
    DemuxTopic.cxx
    FlatbufferReader.cxx
    HDFFile.cxx
    HDFWriterModule.cxx
    MsgQueue.cxx
    KafkaW.cxx
    helper.cxx
    logger.cxx
    json.cxx
    uri.cxx
    h5.cxx
    date/tz.cpp
    date/tzblobs.cpp
    schemas/f142/f142_rw.cxx
    schemas/f142/f142_synth.cxx
    schemas/ev42/ev42_rw.cxx
    schemas/ev42/ev42_synth.cxx
    MainOpt.cxx
    ${FMT_SRC}
    ${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx
)

set(kafka_to_nexus_INC
    CommandHandler.h
    CommandListener.h
    commandproducer.h
    DemuxTopic.h
    Errors.hpp
    FileWriterTask.h
    FlatbufferReader.h
    h5.h
    HDFFile.h
    HDFWriterModule.h
    helper.h
    json.h
    kafka_util.h
    kafka-to-nexus.h
    KafkaW.h
    logger.h
    MainOpt.h
    Master_handler.h
    Master.h
    Msg.h
    MsgParser.h
    ProcessMessageResult.h
    Report.hpp
    Source.h
    Status.hpp
    StatusWriter.hpp
    Streamer.hpp
    StreamMaster.hpp
    TimeDifferenceFromMessage.h
    TopicReader.h
    uri.h
    utils.h
    date/date.h
    date/ios.h
    date/tz_private.h
    date/tz.h
    date/tzblobs.h
    schemas/ev42/ev42_synth.h
    schemas/f142/f142_synth.h
)

set(tgt "kafka_to_nexus__objects")
add_library(${tgt} OBJECT
    ${kafka_to_nexus_SRC}
    ${kafka_to_nexus_INC}
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
    ${JEMALLOC_LIBRARIES}
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



set(tgt "mpi-worker")
set(sources
mpi-worker.cxx
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



if (have_gtest)
    add_subdirectory(tests)
endif()
