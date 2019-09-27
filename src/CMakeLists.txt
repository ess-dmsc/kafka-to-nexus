if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug")
  message(STATUS "CMAKE_BUILD_TYPE was not specified")
endif()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

find_package(fmt REQUIRED)
find_package(RdKafka REQUIRED)
find_package(HDF5_SIMPLE REQUIRED)  # the built in FindHDF5 does not handle 1.10 correctly
find_package(FlatBuffers REQUIRED)
find_package(h5cpp REQUIRED)
find_package(StreamingDataTypes COMPONENTS 2246f84b16423b7ac18e901bf4c43a006fe635dc)
find_package(Boost COMPONENTS filesystem system REQUIRED)
find_package(Date REQUIRED)
find_package(GraylogLogger)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -fPIC -g -D_GLIBCXX_USE_NANOSLEEP")
if (DEFINED _GLIBCXX_USE_CXX11_ABI)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=${_GLIBCXX_USE_CXX11_ABI}")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -fno-inline -ggdb -D_GLIBCXX_USE_NANOSLEEP")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -D_GLIBCXX_USE_NANOSLEEP")

if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.9.3")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=auto")
endif()

set(compile_defs_common "")


set(path_include_common
        ${RDKAFKA_INCLUDE_DIR}
        ${DATE_INCLUDE_DIR}
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
        ${DATE_LIBRARY}
        pthread
        z
        )

list(APPEND compile_defs_common "HAS_REMOTE_API=0")

if (${GRAYLOGLOGGER_FOUND} AND ${USE_GRAYLOG_LOGGER})
  message(STATUS "Using graylog_logger")
  list(APPEND compile_defs_common "HAVE_GRAYLOG_LOGGER=1")
  list(APPEND path_include_common ${GRAYLOGLOGGER_INCLUDE_DIR})
  list(APPEND libraries_common ${GRAYLOGLOGGER_LIBRARY})
elseif (NOT GRAYLOGLOGGER_FOUND AND USE_GRAYLOG_LOGGER)
  message(WARNING "graylog_logger requested but not found")
endif()

set(kafka_to_nexus_SRC
        Status.cpp
        StatusWriter.cpp
        Errors.cpp
        Streamer.cpp
        StreamMaster.cpp
        Master.cpp
        logger.cpp
        CommandListener.cpp
        CommandHandler.cpp
        FileWriterTask.cpp
        Source.cpp
        DemuxTopic.cpp
        EventLogger.cpp
        FlatbufferReader.cpp
        HDFFile.cpp
        HDFWriterModule.cpp
        KafkaW/Consumer.cpp
        KafkaW/Producer.cpp
        KafkaW/ProducerTopic.cpp
        KafkaW/ConsumerFactory.cpp
        helper.cpp
        URI.cpp
        h5.cpp
        FlatbufferMessage.cpp
        MainOpt.cpp
        CLIOptions.cpp
        NeXusDataset.cpp
        StreamMaster.cpp)

set(kafka_to_nexus_INC
        CommandHandler.h
        CommandListener.h
        DemuxTopic.h
        Errors.h
        EventLogger.h
        FileWriterTask.h
        FlatbufferReader.h
        h5.h
        HDFFile.h
        HDFWriterModule.h
        helper.h
        json.h
        KafkaW/BrokerSettings.h
        KafkaW/Consumer.h
        KafkaW/Producer.h
        KafkaW/ProducerTopic.h
        KafkaW/PollStatus.h
        KafkaW/ProducerStats.h
        KafkaW/ProducerMessage.h
        KafkaW/KafkaEventCb.h
        KafkaW/MetadataException.h
        KafkaW/ConsumerFactory.h
        logger.h
        MainOpt.h
        Master.h
        Msg.h
        FlatbufferMessage.h
        ProcessMessageResult.h
        Report.h
        Source.h
        Status.h
        StatusWriter.h
        Streamer.h
        StreamerOptions.h
        StreamMaster.h
        URI.h
        CLIOptions.h
        NeXusDataset.h
        KafkaW/ConfigureKafka.cpp
        KafkaW/ConfigureKafka.h
        StreamsController.h
        CommandParser.h
        CommandParser.cpp
        )

set(tgt "kafka_to_nexus__objects")
add_library(${tgt} OBJECT
        ${kafka_to_nexus_SRC}
        ${kafka_to_nexus_INC}
        )

add_dependencies(${tgt} flatbuffers_generate)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common} ${VERSION_INCLUDE_DIR})


set(tgt "send-command")
set(sources
        send-command.cpp
        $<TARGET_OBJECTS:kafka_to_nexus__objects>
        )
add_executable(${tgt} EXCLUDE_FROM_ALL ${sources})
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common} ${VERSION_INCLUDE_DIR})
target_link_libraries(${tgt} ${libraries_common}  fmt::fmt
        )

set(WRITER_MODULES "" CACHE INTERNAL "WRITER_MODULES")

add_subdirectory(schemas)

set(tgt "kafka-to-nexus")
set(sources
        kafka-to-nexus.cpp
        $<TARGET_OBJECTS:kafka_to_nexus__objects>
        ${WRITER_MODULES}
        )
add_executable(${tgt} ${sources})
add_dependencies(${tgt} flatbuffers_generate)
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common} ${VERSION_INCLUDE_DIR})
target_link_libraries(${tgt} ${libraries_common} fmt::fmt)

option(BUILD_TESTS "Build unit tests" ON)
if (BUILD_TESTS)
  add_subdirectory(tests)
endif()
