set(fb_metadata_SRC
    NDAr_ExtractorTests.cpp
    TemplateExtractorTests.cpp
    tdct_ExtractorTests.cpp
    ev42_ExtractorTests.cpp
    ev44_ExtractorTests.cpp
    senv_ExtractorTests.cpp
    ns10_ExtractorTests.cpp
    ReaderRegistrationTests.cpp
    )

add_library(fb_metadata OBJECT ${fb_metadata_SRC})
target_include_directories(fb_metadata PRIVATE .. ../../)
target_compile_definitions(fb_metadata PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")
