set(writer_module_SRC
    NDAr_WriterTests.cpp
    tdct_WriterTests.cpp
    hs00_WriterTests.cpp
    ev42_WriterTests.cpp
    ev44_WriterTests.cpp
    senv_WriterTests.cpp
    se00_WriterTests.cpp
    ns10_WriterTests.cpp
    f142_WriterTests.cpp
    ep00_WriterTests.cpp
    TemplateWriterTests.cpp
    WriterRegistrationTests.cpp
    f144_WriterTests.cpp
    al00_WriterTests.cpp
    ep01_WriterTests.cpp
)

add_library(writer_module OBJECT ${writer_module_SRC})
target_include_directories(writer_module PRIVATE .. ../../)
target_compile_definitions(writer_module PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")
