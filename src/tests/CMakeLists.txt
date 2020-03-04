find_package(trompeloeil REQUIRED)

get_filename_component(TEST_DATA_PATH "data/" ABSOLUTE)

add_subdirectory(writer_module)
add_subdirectory(fb_metadata)

set(UnitTests_SRC
        UnitTests.cpp
        HDFFileAttributesTests.cpp
        helpers/HDFFileTestHelper.cpp
        JsonTests.cpp
        URITests.cpp
        CommandHandlerTests.cpp
        NeXusDatasetTests.cpp
    MessageTests.cpp
        EventLoggerTests.cpp
        FileWriterTaskTests.cpp
        SourceTests.cpp
        ProducerTests.cpp
        ProducerDeliveryTests.cpp
        ConsumerTests.cpp
        StreamMasterTests.cpp
        CommandParserTests.cpp
        ExtensibleDatasetTests.cpp
        Metrics/MetricsRegistrarTest.cpp
        Metrics/MetricTest.cpp
        Metrics/CarbonConnectionTest.cpp
        Metrics/CarbonTestServer.cpp
        Metrics/MetricsReporterTest.cpp
        Metrics/LogSinkTest.cpp
        MasterTests.cpp
    $<TARGET_OBJECTS:writer_module>
        $<TARGET_OBJECTS:fb_metadata>
    DataMessageWriterTests.cpp MetaDataQueryTests.cpp Stream/PartitionFilterTest.cpp Stream/SourceFilterTest.cpp)

set(UnitTests_INC
        StreamerTestMocks.h
        helpers/HDFFileTestHelper.h
        helpers/KafkaWMocks.h
        helpers/FakeStreamMaster.h
        Metrics/CarbonTestServer.h
        Metrics/MockSink.h
        Metrics/MockReporter.h
        helpers/SetExtractorModule.h StatusReporterTests.cpp)

add_executable(UnitTests
        ${UnitTests_SRC}
        ${UnitTests_INC}
        $<TARGET_OBJECTS:kafka_to_nexus__objects>
        ${WRITER_MODULES}
        ${FB_METADATA_EXTRACTORS}
        )

target_compile_definitions(UnitTests PRIVATE ${compile_defs_common})
target_include_directories(UnitTests PRIVATE ${path_include_common} ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(UnitTests
        trompeloeil::trompeloeil
        ${libraries_common}
        ${CONAN_LIBS_GTEST}
        )

target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")

enable_testing()
add_test(AllTests UnitTests)
