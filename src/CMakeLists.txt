# Relative to build root
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

find_package(fmt REQUIRED)
find_package(RdKafka REQUIRED)
find_package(HDF5_SIMPLE REQUIRED)  # the built in FindHDF5 does not handle 1.10 correctly
find_package(FlatBuffers REQUIRED)
find_package(h5cpp REQUIRED)
find_package(StreamingDataTypes COMPONENTS d2247ffd906ebeeb68c9b8a2ea9f1d36c1d88a46)
find_package(GraylogLogger)
find_package(GitCommitExtract)
find_package(h5cpp REQUIRED)
find_package(Boost COMPONENTS filesystem system REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -fPIC -g -D_GLIBCXX_USE_NANOSLEEP")
if (DEFINED _GLIBCXX_USE_CXX11_ABI)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=${_GLIBCXX_USE_CXX11_ABI}")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11 -O0 -fno-inline -ggdb -D_GLIBCXX_USE_NANOSLEEP")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++11 -O3 -D_GLIBCXX_USE_NANOSLEEP")

if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.9.3")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=auto")
endif()

set(compile_defs_common "")


set(path_include_common
  ${RDKAFKA_INCLUDE_DIR}
  ${HDF5_INCLUDE_DIRS}
  ${FLATBUFFERS_INCLUDE_DIR}
  ${H5CPP_INCLUDE_DIR}
  ${Boost_INCLUDE_DIRS}
  ${CURL_INCLUDE_DIRS}
  ${PROJECT_SOURCE_DIR}/src
  ${PROJECT_BINARY_DIR}/src
)

set(libraries_common
  ${RDKAFKA_LIBRARIES}
  ${HDF5_C_LIBRARIES}
  ${H5CPP_LIBRARIES}
  ${CURL_LIBRARIES}
  ${Boost_LIBRARIES}
  pthread
  z
)

list(APPEND compile_defs_common "HAS_REMOTE_API=0")

if (${GRAYLOGLOGGER_FOUND} AND ${USE_GRAYLOG_LOGGER})
  message(STATUS "Using graylog_logger")
  list(APPEND compile_defs_common "HAVE_GRAYLOG_LOGGER=1")
  list(APPEND path_include_common ${GRAYLOGLOGGER_INCLUDE_DIR})
  list(APPEND libraries_common libgraylog_logger)
elseif (NOT GRAYLOGLOGGER_FOUND AND USE_GRAYLOG_LOGGER)
  message(WARNING "graylog_logger requested but not found")
endif()

set(kafka_to_nexus_SRC
  Status.cpp
  StatusWriter.cpp
  Errors.cpp
  Streamer.cpp
  kafka_util.cpp
  Master.cpp
  CommandListener.cpp
  CommandHandler.cpp
  TimeDifferenceFromMessage.cpp
  FileWriterTask.cpp
  Source.cpp
  DemuxTopic.cpp
  FlatbufferReader.cpp
  HDFFile.cpp
  HDFWriterModule.cpp
  KafkaW/BrokerSettings.cpp
  KafkaW/TopicSettings.cpp
  KafkaW/KafkaW.cpp
  KafkaW/Msg.cpp
  KafkaW/PollStatus.cpp
  KafkaW/Consumer.cpp
  KafkaW/Producer.cpp
  KafkaW/ProducerTopic.cpp
  helper.cpp
  logger.cpp
  uri.cpp
  h5.cpp
  date/tz.cpp
  date/tzblobs.cpp
  MainOpt.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx
  CLIOptions.cpp)

set(kafka_to_nexus_INC
  CommandHandler.h
  CommandListener.h
  commandproducer.h
  DemuxTopic.h
  Errors.h
  FileWriterTask.h
  FlatbufferReader.h
  h5.h
  HDFFile.h
  HDFWriterModule.h
  helper.h
  json.h
  kafka_util.h
  kafka-to-nexus.h
  KafkaW/BrokerSettings.h
  KafkaW/TopicSettings.h
  KafkaW/KafkaW.h
  KafkaW/Msg.h
  KafkaW/PollStatus.h
  KafkaW/Consumer.h
  KafkaW/Producer.h
  KafkaW/ProducerTopic.h
  logger.h
  MainOpt.h
  Master.h
  Msg.h
  MsgParser.h
  ProcessMessageResult.h
  Report.h
  Source.h
  Status.h
  StatusWriter.h
  Streamer.h
  StreamerOptions.h
  StreamMaster.h
  TimeDifferenceFromMessage.h
  TopicReader.h
  uri.h
  date/date.h
  date/ios.h
  date/tz_private.h
  date/tz.h
  date/tzblobs.h
  CLIOptions.h
)

set(tgt "kafka_to_nexus__objects")
add_library(${tgt} STATIC
  ${kafka_to_nexus_SRC}
  ${kafka_to_nexus_INC}
  $<TARGET_OBJECTS:template_module>
  $<TARGET_OBJECTS:ev42_module>
  $<TARGET_OBJECTS:f142_module>
  $<TARGET_OBJECTS:senv_module>
)
target_link_libraries(${tgt} ${libraries_common} fmt::fmt)

add_dependencies(${tgt} flatbuffers_generate)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common})


set(tgt "send-command")
set(sources
        send-command.cpp
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} git_commit_current)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common})
target_link_libraries(${tgt} kafka_to_nexus__objects)


set(tgt "kafka-to-nexus")
set(sources
        kafka-to-nexus.cpp
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} flatbuffers_generate)
add_dependencies(${tgt} git_commit_current)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common})
target_link_libraries(${tgt} kafka_to_nexus__objects)

find_package(GTest)

add_subdirectory(schemas)

if (${GTEST_FOUND})
  add_subdirectory(tests)
endif()
