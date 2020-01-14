set(fb_metadata_SRC
    AreaDetectorTests.cpp
    ChopperTimeStampWriterTests.cpp
    EventHistogramWriterTests.cpp
    EventWriterTests.cpp
    FastSampleEnvironmentWriterTests.cpp
    NicosCacheWriterTests.cpp
    ReaderRegistrationTests.cpp
    Schema_f142Tests.cpp
    TemplateWriterTests.cpp
    )

add_library(fb_metadata OBJECT ${fb_metadata_SRC})
target_include_directories(fb_metadata PRIVATE ..)
