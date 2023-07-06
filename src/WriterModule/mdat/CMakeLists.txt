set(mdat_SRC
        mdat_Writer.cpp
)

set(mdat_INC
        mdat_Writer.h
        ConnectionStatusDatasets.h
)

create_writer_module(mdat)
