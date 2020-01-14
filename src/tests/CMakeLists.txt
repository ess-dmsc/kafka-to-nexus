find_package(Trompeloeil REQUIRED)

get_filename_component(TEST_DATA_PATH "data/" ABSOLUTE)

add_subdirectory(writer_module)
#add_subdirectory(fb_metadata)

set(UnitTests_SRC
        UnitTests.cpp
        HDFFileAttributesTests.cpp
        helpers/HDFFileTestHelper.cpp
        JsonTests.cpp
        StatusTests.cpp
        URITests.cpp
        StatusWriterTests.cpp
        CommandHandlerTests.cpp
        NeXusDatasetTests.cpp
        DemuxerTests.cpp
        MessageTests.cpp
        EventLoggerTests.cpp
        MainOptTests.cpp
        FileWriterTaskTests.cpp
        SourceTests.cpp
        ProducerTests.cpp
        ProducerDeliveryTests.cpp
        helpers/ev42_synth.cpp
        helpers/f142_synth.cpp
        ConsumerTests.cpp
        StreamMasterTests.cpp
        CommandParserTests.cpp
        ExtensibleDatasetTests.cpp
        MasterTests.cpp
        StreamerTests.cpp
        $<TARGET_OBJECTS:writer_module>
#        $<TARGET_OBJECTS:fb_metadata>
    )

set(UnitTests_INC
        StreamerTestMocks.h
        helpers/HDFFileTestHelper.h
        helpers/ev42_synth.h
        helpers/f142_synth.h
        helpers/KafkaWMocks.h
        helpers/FakeStreamMaster.h
        )

add_executable(UnitTests
        ${UnitTests_SRC}
        ${UnitTests_INC}
        $<TARGET_OBJECTS:kafka_to_nexus__objects>
        ${WRITER_MODULES}
        ${FB_METADATA_EXTRACTORS}
        )

add_dependencies(UnitTests flatbuffers_generate)
target_compile_definitions(UnitTests PRIVATE ${compile_defs_common})
target_include_directories(UnitTests PRIVATE ${path_include_common} ${Trompeloeil_INCLUDE_DIR})
target_link_libraries(UnitTests
        ${libraries_common}
        fmt::fmt
        ${CONAN_LIBS_GTEST}
        )

target_compile_definitions(UnitTests PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")

enable_testing()
add_test(AllTests UnitTests)
