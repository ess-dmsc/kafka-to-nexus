get_filename_component(TEST_DATA_PATH "data/" ABSOLUTE)

set(UnitTests_SRC
        Metrics/LogSinkTest.cpp
        Metrics/MetricsReporterTest.cpp
        Metrics/CarbonTestServer.cpp
        Metrics/CarbonConnectionTest.cpp
        Metrics/MetricsRegistrarTest.cpp
        Metrics/MetricTest.cpp
        JobCreatorTests.cpp
        HDFFileTest.cpp
        FileWriterTaskTests.cpp
        JsonConfig/FieldTest.cpp
        JsonConfig/FieldHandlerTest.cpp
        MultiVectorTest.cpp
        HDFAttributesTest.cpp
        KafkaToNexusTests.cpp
        Stream/PartitionFilterTest.cpp
        Stream/SourceFilterTest.cpp
        Stream/MessageWriterTests.cpp
        Stream/TopicTests.cpp
        Stream/PartitionTests.cpp
        MessageTests.cpp
        URITests.cpp
        ProducerDeliveryTests.cpp
        SourceTests.cpp
        MasterTests.cpp
        HDFFileAttributesTests.cpp
        JsonTests.cpp
        ProducerTests.cpp
        ConsumerTests.cpp
        CommandSystem/CommandListenerTests.cpp
        CommandSystem/CommandParserTests.cpp
        CommandSystem/CommandHandlerTests.cpp
        StatusReporterTests.cpp
        UnitTests.cpp
        ThreadedExecutorTests.cpp
        NeXusDataset/NeXusDatasetTests.cpp
        NeXusDataset/ExtensibleDatasetTests.cpp
        HelperTests.cpp
        AccessMessageMetadata/ev42_ExtractorTests.cpp
        AccessMessageMetadata/tdct_ExtractorTests.cpp
        AccessMessageMetadata/ns10_ExtractorTests.cpp
        AccessMessageMetadata/senv_ExtractorTests.cpp
        AccessMessageMetadata/TemplateExtractorTests.cpp
        AccessMessageMetadata/NDAr_ExtractorTests.cpp
        AccessMessageMetadata/ReaderRegistrationTests.cpp
        WriterModule/hs00_WriterTests.cpp
        WriterModule/ep01_WriterTests.cpp
        WriterModule/se00_WriterTests.cpp
        WriterModule/ns10_WriterTests.cpp
        WriterModule/tdct_WriterTests.cpp
        WriterModule/TemplateWriterTests.cpp
        WriterModule/senv_WriterTests.cpp
        WriterModule/NDAr_WriterTests.cpp
        WriterModule/al00_WriterTests.cpp
        WriterModule/ev44_WriterTests.cpp
        WriterModule/f144_WriterTests.cpp
        WriterModule/WriterRegistrationTests.cpp
        WriterModule/f142_WriterTests.cpp
        WriterModule/mdat_WriterTests.cpp
        WriterModule/ev42_WriterTests.cpp
        TimeUtilityTest.cpp
        MetaData/TrackerTest.cpp
        MetaData/ValueTest.cpp
        MetaData/HDF5DataTest.cpp
        helpers/RunStartStopHelpers.cpp
        helpers/StatusHelpers.cpp
        helpers/HDFFileTestHelper.cpp
        HDFOperationsTests.cpp
        )

find_package(concurrentqueue)
find_package(readerwriterqueue)
find_package(RdKafka REQUIRED)
find_package(HDF5 REQUIRED)
find_package(flatbuffers)
if (NOT flatbuffers_FOUND)
    find_package(Flatbuffers)
endif()
find_package(fmt REQUIRED)
find_package(h5cpp REQUIRED)
find_package(streaming-data-types)
find_package(date REQUIRED)
find_package(asio REQUIRED)
find_package(graylog-logger REQUIRED)
find_package(trompeloeil REQUIRED)

set(libraries_common
        ${CURL_LIBRARIES}
        date::date
        fmt::fmt
        RdKafka::rdkafka++
        hdf5::hdf5
        h5cpp::h5cpp
        asio::asio
        pthread
        z
        graylog-logger::graylog-logger
        readerwriterqueue::readerwriterqueue
        concurrentqueue::concurrentqueue
        )

add_executable(UnitTests ${UnitTests_SRC})
target_include_directories(UnitTests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(UnitTests PRIVATE filewriter_lib ${libraries_common} trompeloeil::trompeloeil ${CONAN_LIBS_GTEST}
        )
target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")

enable_testing()
add_test(AllTests UnitTests)
