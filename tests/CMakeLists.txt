get_filename_component(TEST_DATA_PATH "${CMAKE_CURRENT_SOURCE_DIR}/data/" ABSOLUTE)

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
    AccessMessageMetadata/tdct_ExtractorTests.cpp
    AccessMessageMetadata/TemplateExtractorTests.cpp
    AccessMessageMetadata/ReaderRegistrationTests.cpp
    WriterModule/ep01_WriterTests.cpp
    WriterModule/se00_WriterTests.cpp
    WriterModule/tdct_WriterTests.cpp
    WriterModule/TemplateWriterTests.cpp
    WriterModule/al00_WriterTests.cpp
    WriterModule/ev44_WriterTests.cpp
    WriterModule/f144_WriterTests.cpp
    WriterModule/WriterRegistrationTests.cpp
    WriterModule/mdat_WriterTests.cpp
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

find_package(trompeloeil REQUIRED)
find_package(GTest REQUIRED)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(UnitTests ${UnitTests_SRC})
target_include_directories(UnitTests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")
target_link_libraries(UnitTests
  PRIVATE
    filewriter_lib
    ${COMMON_LIBS}
    trompeloeil::trompeloeil
    GTest::gtest
)
enable_coverage_for_target(UnitTests)

add_test(NAME AllTests COMMAND ${CMAKE_BINARY_DIR}/bin/UnitTests --gtest_output=xml:junit.xml)
