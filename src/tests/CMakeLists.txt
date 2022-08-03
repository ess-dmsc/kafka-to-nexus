find_package(trompeloeil REQUIRED)

get_filename_component(TEST_DATA_PATH "data/" ABSOLUTE)

add_subdirectory(WriterModule)
add_subdirectory(AccessMessageMetadata)
add_subdirectory(NeXusDataset)

set(UnitTests_SRC
        UnitTests.cpp
        HDFFileAttributesTests.cpp
        HDFOperationsTests.cpp
        MultiVectorTest.cpp
        HDFAttributesTest.cpp
        helpers/HDFFileTestHelper.cpp
        helpers/RunStartStopHelpers.cpp
        JsonTests.cpp
        URITests.cpp
        CommandHandlerTests.cpp
        MessageTests.cpp
        FileWriterTaskTests.cpp
        SourceTests.cpp
        ProducerDeliveryTests.cpp
        Metrics/MetricsRegistrarTest.cpp
        Metrics/MetricTest.cpp
        Metrics/CarbonConnectionTest.cpp
        Metrics/CarbonTestServer.cpp
        Metrics/MetricsReporterTest.cpp
        Metrics/LogSinkTest.cpp
        Stream/PartitionFilterTest.cpp
        Stream/MessageWriterTests.cpp
        Stream/SourceFilterTest.cpp
        Stream/PartitionTests.cpp
        Stream/TopicTests.cpp
        $<TARGET_OBJECTS:writer_module>
        $<TARGET_OBJECTS:fb_metadata>
        ThreadedExecutorTests.cpp
        $<TARGET_OBJECTS:NeXusDatasetTests>
        StatusReporterTests.cpp
        helpers/StatusHelpers.cpp
        HelperTests.cpp
        CommandSystem/CommandParserTests.cpp
        CommandSystem/CommandListenerTests.cpp
        TimeUtilityTest.cpp
        JsonConfig/FieldTest.cpp
        JsonConfig/FieldHandlerTest.cpp
        MetaData/ValueTest.cpp
        JobCreatorTests.cpp
        HDFFileTest.cpp
        MasterTests.cpp)

set(UnitTests_INC
        helpers/HDFFileTestHelper.h
        helpers/RdKafkaMocks.h
        helpers/KafkaMocks.h
        helpers/FakeStreamController.h
        helpers/RunStartStopHelpers.h
        Metrics/CarbonTestServer.h
        Metrics/MockSink.h
        Metrics/MockReporter.h
        helpers/SetExtractorModule.h
        helpers/StatusHelpers.h MetaData/TrackerTest.cpp MetaData/HDF5DataTest.cpp)

add_executable(UnitTests EXCLUDE_FROM_ALL
        ${UnitTests_SRC}
        ${UnitTests_INC}
        $<TARGET_OBJECTS:NeXusDataset>
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

# Link stdc++fs or c++experimental to get std::experimental::filesystem when necessary
target_link_libraries(UnitTests $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,9.0>>:stdc++fs>)
target_link_libraries(UnitTests $<$<AND:$<CXX_COMPILER_ID:AppleClang>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,11.0>>:c++fs>)

target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")

enable_testing()
add_test(AllTests UnitTests)
