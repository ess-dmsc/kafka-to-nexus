set(NeXusDataset_SRC
        ExtensibleDatasetTests.cpp
        NeXusDatasetTests.cpp
        )

add_library(NeXusDatasetTests OBJECT ${NeXusDataset_SRC})
target_include_directories(NeXusDatasetTests PRIVATE .. ../../)
