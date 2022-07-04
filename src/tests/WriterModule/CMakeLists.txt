set(writer_module_SRC
    NDAr_WriterTests.cpp
    tdct_WriterTests.cpp
    hs00_WriterTests.cpp
    ev42_WriterTests.cpp
    senv_WriterTests.cpp
    ns10_WriterTests.cpp
    f142_WriterTests.cpp
    ep00_WriterTests.cpp
    TemplateWriterTests.cpp
    WriterRegistrationTests.cpp
        scal_WriterTests.cpp pvAl_WriterTests.cpp pvCn_WriterTests.cpp)

add_library(writer_module OBJECT ${writer_module_SRC})
target_include_directories(writer_module PRIVATE .. ../../)
target_compile_definitions(writer_module PRIVATE TEST_DATA_PATH="${TEST_DATA_PATH}")
