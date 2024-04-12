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
        HDFAttributes.cpp
        Kafka/Consumer.cpp
        Kafka/Producer.cpp
        Kafka/ProducerTopic.cpp
        Kafka/ConsumerFactory.cpp
        Kafka/MetaDataQuery.cpp
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
        Status/StatusService.cpp
        Stream/MessageWriter.cpp
        Stream/SourceFilter.cpp
        Stream/Partition.cpp
        Stream/Topic.cpp
        HDFOperations.cpp
        HDFVersionCheck.cpp
        CommandSystem/CommandListener.cpp
        TimeUtility.cpp
        JsonConfig/Field.cpp
        JsonConfig/FieldHandler.cpp
        SetThreadName.cpp
        MetaData/Tracker.cpp
        MetaData/HDF5DataWriter.cpp WriterModuleBase.cpp WriterModule/mdat/mdat_Writer.cpp)

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
        HDFAttributes.h
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
        kafka-to-nexus.h
        logger.h
        MainOpt.h
        Master.h
        Msg.h
        FlatbufferMessage.h
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
        Status/StatusService.h
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
        ModuleSettings.h
        ModuleHDFInfo.h
        CommandSystem/CommandListener.h
        CommandSystem/FeedbackProducerBase.h
        CommandSystem/JobListener.h
        JsonConfig/Field.h
        JsonConfig/FieldHandler.h
        MultiVector.h
        SetThreadName.h
        MetaData/Tracker.h
        MetaData/Value.h
        MetaData/ValueInternal.h
        MetaData/HDF5DataWriter.h WriterModule/mdat/mdat_Writer.h)



add_library(filewriter_lib SHARED ${kafka_to_nexus_SRC} ${kafka_to_nexus_INC})
target_link_libraries(filewriter_lib PRIVATE ${CONAN_LIBS})

target_include_directories(filewriter_lib PUBLIC .)

#add_subdirectory(WriterModule)
#add_subdirectory(AccessMessageMetadata)
#add_subdirectory(NeXusDataset)
#
#set(sources
#        $<TARGET_OBJECTS:kafka_to_nexus__objects>
#        $<TARGET_OBJECTS:NeXusDataset>
#        ${WRITER_MODULES}
#        ${FB_METADATA_EXTRACTORS})
#add_executable(kafka-to-nexus ${sources} kafka-to-nexus.cpp)
#target_compile_definitions(kafka-to-nexus PRIVATE ${compile_defs_common})
#target_include_directories(kafka-to-nexus PRIVATE ${path_include_common} ${VERSION_INCLUDE_DIR})
#target_link_libraries(kafka-to-nexus ${libraries_common})
#
## Link stdc++fs or c++experimental to get std::experimental::filesystem when necessary
#target_link_libraries(kafka-to-nexus $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,9.0>>:stdc++fs>)
#target_link_libraries(kafka-to-nexus $<$<AND:$<CXX_COMPILER_ID:AppleClang>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,11.0>>:c++fs>)
#
#add_subdirectory(tests)
