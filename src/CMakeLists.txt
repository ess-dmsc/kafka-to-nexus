if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug")
  message(STATUS "CMAKE_BUILD_TYPE was not specified")
endif()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

find_package(librdkafka REQUIRED)
find_package(hdf5 REQUIRED)
find_package(flatbuffers)
if (NOT flatbuffers_FOUND)
  find_package(Flatbuffers)
endif()
find_package(fmt REQUIRED)
find_package(h5cpp REQUIRED)
find_package(streaming-data-types)
find_package(date REQUIRED)
find_package(asio REQUIRED)

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
        asio::asio
        pthread
        z
        )

list(APPEND compile_defs_common "HAS_REMOTE_API=0")

set(USE_GRAYLOG_LOGGER ON CACHE BOOL "Set to OFF to disable log reporting to graylog")
if (${USE_GRAYLOG_LOGGER})
  find_package(spdlog-graylog REQUIRED)
  message(STATUS "Using graylog_logger")
  list(APPEND compile_defs_common "HAVE_GRAYLOG_LOGGER=1")
  list(APPEND libraries_common spdlog-graylog::spdlog-graylog)
else()
  find_package(spdlog REQUIRED)
  list(APPEND libraries_common spdlog::spdlog)
endif()

set(kafka_to_nexus_SRC
        StreamController.cpp
        Master.cpp
        logger.cpp
        CommandSystem/JobListener.cpp
        CommandSystem/CommandListener.cpp
        CommandSystem/FeedbackProducer.cpp
        CommandSystem/Handler.cpp
        CommandSystem/Parser.cpp
        JobCreator.cpp
        FileWriterTask.cpp
        Source.cpp
        FlatbufferReader.cpp
        HDFFile.cpp
        Kafka/Consumer.cpp
        Kafka/Producer.cpp
        Kafka/ProducerTopic.cpp
        Kafka/ConsumerFactory.cpp
        Kafka/MetaDataQuery.cpp
        Kafka/MetaDataQueryImpl.cpp
        helper.cpp
        URI.cpp
        FlatbufferMessage.cpp
        MainOpt.cpp
        CLIOptions.cpp
        StreamController.cpp
        WriterRegistrar.cpp
        Metrics/Reporter.cpp
        Metrics/Registrar.cpp
        Metrics/Metric.cpp
        Metrics/CarbonInterface.cpp
        Metrics/CarbonConnection.cpp
        Metrics/LogSink.cpp
        Metrics/CarbonSink.cpp
        Status/StatusReporterBase.cpp
        Stream/PartitionFilter.cpp
        Status/StatusReporter.cpp
        Stream/MessageWriter.cpp
        Stream/SourceFilter.cpp
        Stream/Partition.cpp
        Stream/Topic.cpp
        HDFOperations.cpp
        HDFVersionCheck.cpp
        CommandSystem/CommandListener.cpp
        TimeUtility.cpp
        CommandSystem/IdChecker.cpp
        WriterModuleConfig/Field.cpp
        WriterModuleConfig/FieldHandler.cpp
        WriterModuleBase.cpp)

set(kafka_to_nexus_INC
        JobCreator.h
        CommandSystem/Commands.h
        CommandSystem/JobListener.h
        CommandSystem/CommandListener.h
        CommandSystem/FeedbackProducer.h
        CommandSystem/Handler.h
        CommandSystem/Parser.h
        FileWriterTask.h
        FlatbufferReader.h
        HDFFile.h
        WriterModuleBase.h
        helper.h
        json.h
        Kafka/BrokerSettings.h
        Kafka/Consumer.h
        Kafka/Producer.h
        Kafka/ProducerTopic.h
        Kafka/PollStatus.h
        Kafka/ProducerStats.h
        Kafka/ProducerMessage.h
        Kafka/KafkaEventCb.h
        Kafka/MetadataException.h
        Kafka/ConsumerFactory.h
        Kafka/MetaDataQuery.h
        Kafka/MetaDataQueryImpl.h
        logger.h
        MainOpt.h
        Master.h
        Msg.h
        FlatbufferMessage.h
        Filesystem.h
        Source.h
        StreamerOptions.h
        StreamController.h
        URI.h
        CLIOptions.h
        Kafka/ConfigureKafka.cpp
        Kafka/ConfigureKafka.h
        WriterRegistrar.h
        Metrics/Registrar.h
        Metrics/Metric.h
        Metrics/CarbonInterface.h
        Metrics/CarbonConnection.h
        Metrics/Sink.h
        Metrics/LogSink.h
        Metrics/CarbonSink.h
        Metrics/InternalMetric.h
        Metrics/Reporter.h
        Status/StatusInfo.h
        Status/StatusReporter.h
        Status/StatusReporter.cpp
        Status/StatusReporterBase.h
        Stream/PartitionFilter.h
        Status/StatusReporterBase.h
        Stream/MessageWriter.h
        Stream/Message.h
        Stream/SourceFilter.h
        Stream/Partition.h
        Stream/Topic.h
        ThreadedExecutor.h
        TimeUtility.h
        HDFOperations.h
        HDFVersionCheck.h
        StreamHDFInfo.h
        CommandSystem/CommandListener.h
        CommandSystem/FeedbackProducerBase.h
        CommandSystem/IdChecker.h
        WriterModuleConfig/Field.h
        CommandSystem/JobListener.h
        WriterModuleConfig/FieldHandler.h)

add_library(kafka_to_nexus__objects OBJECT
        ${kafka_to_nexus_SRC}
        ${kafka_to_nexus_INC}
        )

target_compile_definitions(kafka_to_nexus__objects PRIVATE ${compile_defs_common})
target_include_directories(kafka_to_nexus__objects PRIVATE ${path_include_common} ${VERSION_INCLUDE_DIR})

set(WRITER_MODULES "" CACHE INTERNAL "WRITER_MODULES")
set(FB_METADATA_EXTRACTORS "" CACHE INTERNAL "FB_METADATA_EXTRACTORS")

add_subdirectory(WriterModule)
add_subdirectory(AccessMessageMetadata)
add_subdirectory(NeXusDataset)

set(sources
        kafka-to-nexus.cpp
        $<TARGET_OBJECTS:kafka_to_nexus__objects>
        $<TARGET_OBJECTS:NeXusDataset>
        ${WRITER_MODULES}
        ${FB_METADATA_EXTRACTORS}
        )
add_executable(kafka-to-nexus ${sources})
target_compile_definitions(kafka-to-nexus PRIVATE ${compile_defs_common})
target_include_directories(kafka-to-nexus PRIVATE ${path_include_common} ${VERSION_INCLUDE_DIR})
target_link_libraries(kafka-to-nexus ${libraries_common})

# Link stdc++fs or c++experimental to get std::experimental::filesystem when necessary
target_link_libraries(kafka-to-nexus $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,9.0>>:stdc++fs>)
target_link_libraries(kafka-to-nexus $<$<AND:$<CXX_COMPILER_ID:AppleClang>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,11.0>>:c++fs>)

option(BUILD_TESTS "Build unit tests" ON)
if (BUILD_TESTS)
  add_subdirectory(tests)
endif()
