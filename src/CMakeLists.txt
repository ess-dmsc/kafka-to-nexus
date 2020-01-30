if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug")
  message(STATUS "CMAKE_BUILD_TYPE was not specified")
endif()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

find_package(librdkafka REQUIRED)
find_package(hdf5 REQUIRED)
find_package(flatbuffers REQUIRED)
find_package(fmt REQUIRED)
find_package(h5cpp REQUIRED)
find_package(streaming-data-types)
find_package(date REQUIRED)

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
        ${CURL_INCLUDE_DIRS}
        ${PROJECT_SOURCE_DIR}/src
        )

set(libraries_common
        ${CURL_LIBRARIES}
        date::date
        fmt::fmt
        librdkafka::librdkafka
        hdf5::hdf5
        h5cpp::h5cpp
        pthread
        z
        )

list(APPEND compile_defs_common "HAS_REMOTE_API=0")

set(USE_GRAYLOG_LOGGER ON CACHE BOOL "Set to OFF to disable log reporting to graylog")
if (${USE_GRAYLOG_LOGGER})
  find_package(graylog-logger REQUIRED)
  message(STATUS "Using graylog_logger")
  list(APPEND compile_defs_common "HAVE_GRAYLOG_LOGGER=1")
  list(APPEND libraries_common graylog-logger::graylog-logger)
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
        JobCreator.cpp
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
        FlatbufferMessage.cpp
        MainOpt.cpp
        CLIOptions.cpp
        NeXusDataset.cpp
        StreamMaster.cpp
        CommandParser.cpp
        StreamsController.cpp
        ExtensibleDataset.cpp
        )

set(kafka_to_nexus_INC
        JobCreator.h
        CommandListener.h
        DemuxTopic.h
        Errors.h
        EventLogger.h
        FileWriterTask.h
        FlatbufferReader.h
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
        ExtensibleDataset.h
    )

set(tgt "kafka_to_nexus__objects")
add_library(${tgt} OBJECT
        ${kafka_to_nexus_SRC}
        ${kafka_to_nexus_INC}
        )

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
target_link_libraries(${tgt} ${libraries_common})

set(WRITER_MODULES "" CACHE INTERNAL "WRITER_MODULES")

add_subdirectory(schemas)

set(tgt "kafka-to-nexus")
set(sources
        kafka-to-nexus.cpp
        $<TARGET_OBJECTS:kafka_to_nexus__objects>
        ${WRITER_MODULES}
        )
add_executable(${tgt} ${sources})
target_compile_definitions(${tgt} PRIVATE ${compile_defs_common})
target_include_directories(${tgt} PRIVATE ${path_include_common} ${VERSION_INCLUDE_DIR})
target_link_libraries(${tgt} ${libraries_common})

option(BUILD_TESTS "Build unit tests" ON)
if (BUILD_TESTS)
  add_subdirectory(tests)
endif()
