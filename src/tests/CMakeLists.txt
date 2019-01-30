find_package(Trompeloeil REQUIRED)

set(UnitTests_SRC
        UnitTests.cpp
        H5Tests.cpp
        HDFFile.cpp
        HDFFileAttributesTest.cpp
        HDFFileTestHelper.cpp
        json.cpp
        StatusTest.cpp
        URITests.cpp
        StatusWriterTests.cpp
        CommandHandlerTests.cpp
        FastSampleEnvironmentWriterTests.cpp
        NeXusDatasets.cpp
        ReaderRegistration.cpp
        WriterRegistration.cpp
        TemplateWriterTests.cpp
        DemuxerTests.cpp
        MessageTest.cpp
        StreamerTest.cpp
        AddReader.cpp
        Schema_f142.cpp
        AreaDetectorTest.cpp
        EventLogger.cpp
        MainOpt.cpp
        EventHistogramWriterTests.cpp
        FileWriterTaskTests.cpp
        SourceTests.cpp
        Producer_tests.cpp
        ProducerDeliveryCb_tests.cpp
        )

set(UnitTests_INC
        MasterMock.h
        HDFFileTestHelper.h
        AddReader.h
        ConsumerTests.cpp KafkaWMocks.h)

add_executable(UnitTests EXCLUDE_FROM_ALL
        ${UnitTests_SRC}
        ${UnitTests_INC}
        $<TARGET_OBJECTS:kafka_to_nexus__objects>
        ${WRITER_MODULES}
        )

add_dependencies(UnitTests flatbuffers_generate)
target_compile_definitions(UnitTests PRIVATE ${compile_defs_common})
target_include_directories(UnitTests PRIVATE ${path_include_common} ${Trompeloeil_INCLUDE_DIR})
target_link_libraries(UnitTests
        ${libraries_common}
        fmt::fmt
        )
add_gtest_to_target(UnitTests)

get_filename_component(TEST_DATA_PATH "data/" ABSOLUTE)
target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")

enable_testing()
add_test(AllTests UnitTests)
