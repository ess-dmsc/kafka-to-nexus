set(kafka_to_nexus_SRC
        FlatbufferReader.cpp
        Metrics/CarbonSink.cpp
        Metrics/Reporter.cpp
        Metrics/CarbonInterface.cpp
        Metrics/Metric.cpp
        Metrics/LogSink.cpp
        Metrics/CarbonConnection.cpp
        Metrics/Registrar.cpp
        SetThreadName.cpp
        Source.cpp
        WriterModuleBase.cpp
        JsonConfig/Field.cpp
        JsonConfig/FieldHandler.cpp
        FileWriterTask.cpp
        Stream/PartitionFilter.cpp
        Stream/MessageWriter.cpp
        Stream/Topic.cpp
        Stream/SourceFilter.cpp
        Stream/Partition.cpp
        TimeUtility.cpp
        helper.cpp
        URI.cpp
        HDFOperations.cpp
        Status/StatusReporter.cpp
        Status/StatusReporterBase.cpp
        Status/StatusService.cpp
        Master.cpp
        JobCreator.cpp
        CommandSystem/JobListener.cpp
        CommandSystem/CommandListener.cpp
        CommandSystem/FeedbackProducer.cpp
        CommandSystem/Handler.cpp
        CommandSystem/Parser.cpp
        Kafka/ProducerTopic.cpp
        Kafka/MetaDataQuery.cpp
        Kafka/Producer.cpp
        Kafka/ConfigureKafka.cpp
        Kafka/ConsumerFactory.cpp
        Kafka/Consumer.cpp
        MainOpt.cpp
        FlatbufferMessage.cpp
        NeXusDataset/NeXusDataset.cpp
        NeXusDataset/EpicsAlarmDatasets.cpp
        NeXusDataset/AdcDatasets.cpp
        NeXusDataset/ExtensibleDataset.cpp
        StreamController.cpp
        logger.cpp
        WriterRegistrar.cpp
        AccessMessageMetadata/NDAr/NDAr_Extractor.cpp
        AccessMessageMetadata/hs00/hs00_Extractor.cpp
        AccessMessageMetadata/ns10/ns10_Extractor.cpp
        AccessMessageMetadata/tdct/tdct_Extractor.cpp
        AccessMessageMetadata/template/TemplateExtractor.cpp
        AccessMessageMetadata/ADAr/ADAr_Extractor.cpp
        AccessMessageMetadata/ev44/ev44_Extractor.cpp
        AccessMessageMetadata/ev42/ev42_Extractor.cpp
        AccessMessageMetadata/f144/f144_Extractor.cpp
        AccessMessageMetadata/f142/f142_Extractor.cpp
        AccessMessageMetadata/se00/se00_Extractor.cpp
        AccessMessageMetadata/al00/al00_Extractor.cpp
        AccessMessageMetadata/senv/senv_Extractor.cpp
        AccessMessageMetadata/ep01/ep01_Extractor.cpp
        WriterModule/NDAr/NDAr_Writer.cpp
        WriterModule/hs00/hs00_Writer.cpp
        WriterModule/hs00/WriterTyped.cpp
        WriterModule/hs00/Slice.cpp
        WriterModule/hs00/HistogramRecord.cpp
        WriterModule/hs00/Shape.cpp
        WriterModule/hs00/Dimension.cpp
        WriterModule/ns10/ns10_Writer.cpp
        WriterModule/tdct/tdct_Writer.cpp
        WriterModule/template/TemplateWriter.cpp
        WriterModule/ADAr/ADAr_Writer.cpp
        WriterModule/ev44/ev44_Writer.cpp
        WriterModule/ev42/ev42_Writer.cpp
        WriterModule/f144/f144_Writer.cpp
        WriterModule/f142_test/f142_TestWriter.cpp
        WriterModule/f142/f142_Writer.cpp
        WriterModule/se00/se00_Writer.cpp
        WriterModule/al00/al00_Writer.cpp
        WriterModule/senv/senv_Writer.cpp
        WriterModule/ep01/ep01_Writer.cpp
        WriterModule/mdat/mdat_Writer.cpp
        HDFVersionCheck.cpp
        CLIOptions.cpp
        MetaData/Tracker.cpp
        MetaData/HDF5DataWriter.cpp
        HDFAttributes.cpp
        HDFFile.cpp
        )

#find_package(concurrentqueue)
#find_package(readerwriterqueue)
#find_package(RdKafka REQUIRED)
#find_package(HDF5 REQUIRED)
#find_package(flatbuffers)
#if (NOT flatbuffers_FOUND)
#    find_package(Flatbuffers)
#endif()
#find_package(fmt REQUIRED)
#find_package(h5cpp REQUIRED)
#find_package(streaming-data-types)
#find_package(date REQUIRED)
#find_package(asio REQUIRED)
#find_package(graylog-logger REQUIRED)
#
#set(libraries_common
#        ${CURL_LIBRARIES}
#        date::date
#        fmt::fmt
#        RdKafka::rdkafka++
#        hdf5::hdf5
#        h5cpp::h5cpp
#        asio::asio
#        pthread
#        z
#        graylog-logger::graylog-logger
#        readerwriterqueue::readerwriterqueue
#        concurrentqueue::concurrentqueue
#        )


add_library(filewriter_lib SHARED ${kafka_to_nexus_SRC})
target_link_libraries(filewriter_lib PUBLIC ${CONAN_LIBS})
target_include_directories(filewriter_lib PUBLIC .)
