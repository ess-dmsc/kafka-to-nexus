find_package(Trompeloeil REQUIRED)

set(UnitTests_SRC
        UnitTests.cpp
        H5Tests.cpp
        HDFFileTests.cpp
        HDFFileAttributesTests.cpp
        HDFFileTestHelper.cpp
        JsonTests.cpp
        StatusTests.cpp
        URITests.cpp
        StatusWriterTests.cpp
        CommandHandlerTests.cpp
        FastSampleEnvironmentWriterTests.cpp
        NeXusDatasetTests.cpp
        ReaderRegistrationTests.cpp
        WriterRegistrationTests.cpp
        TemplateWriterTests.cpp
        DemuxerTests.cpp
        MessageTests.cpp
        StreamerTests.cpp
        AddReader.cpp
        Schema_f142Tests.cpp
        AreaDetectorTests.cpp
        EventLoggerTests.cpp
        MainOptTests.cpp
        EventHistogramWriterTests.cpp
        FileWriterTaskTests.cpp
        SourceTests.cpp
        ProducerTests.cpp
        ProducerDeliveryTests.cpp
        ev42_synth.cpp
        f142_synth.cpp
        ConsumerTests.cpp
        ChopperTimeStampWriterTests.cpp
        NicosCacheWriterTests.cpp
        )

set(UnitTests_INC
        StreamerTestMocks.h
        HDFFileTestHelper.h
        AddReader.h
        ev42_synth.h
        f142_synth.h
        KafkaWMocks.h)

add_executable(UnitTests
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
        ${CONAN_LIBS_GTEST}
        )

get_filename_component(TEST_DATA_PATH "data/" ABSOLUTE)
target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")

enable_testing()
add_test(AllTests UnitTests)
