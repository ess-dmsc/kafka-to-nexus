set(datasets_SRC
        NeXusDataset.cpp
        ExtensibleDataset.cpp
        AdcDatasets.cpp
        )

set(datasets_INC
        NeXusDataset.h
        ExtensibleDataset.h
        AdcDatasets.h
        )

add_library(NeXusDataset OBJECT
                ${datasets_SRC}
                ${datasets_INC}
                )
