get_filename_component(TEST_DATA_PATH "data/" ABSOLUTE)

set(UnitTests_SRC
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
        JsonToFlatbuffersTests.cpp
        ProducerTests.cpp
        ConsumerTests.cpp
        CommandSystem/CommandListenerTests.cpp
        CommandSystem/CommandParserTests.cpp
        CommandSystem/HandlerTests.cpp
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
        WriterModule/da00_WriterTests.cpp
        TimeUtilityTest.cpp
        MetaData/TrackerTest.cpp
        MetaData/ValueTest.cpp
        MetaData/HDF5DataTest.cpp
        helpers/RunStartStopHelpers.cpp
        helpers/StatusHelpers.cpp
        helpers/HDFFileTestHelper.cpp
        HDFOperationsTests.cpp
        CommandSystem/FeedbackProducerTests.cpp
        CommandSystem/JobListenerTests.cpp
)

add_executable(UnitTests ${UnitTests_SRC})
target_include_directories(UnitTests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(UnitTests PRIVATE filewriter_lib ${CONAN_LIBS})
target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")

enable_testing()
add_test(AllTests UnitTests)
