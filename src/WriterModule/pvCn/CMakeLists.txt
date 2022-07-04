set(pvCn_SRC
        pvCn_Writer.cpp
)

set(pvCn_INC
        pvCn_Writer.h
        ConnectionStatusDatasets.h
)

create_writer_module(pvCn)
