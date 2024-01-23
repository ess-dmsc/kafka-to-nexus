set(da00_SRC
        "da00_Writer.cpp"
        "da00_Type.cpp"
)

set(da00_INC
        "da00_Writer.h"
        "da00_Type.h"
        "da00_Edge.h"
        "da00_Variable.h"
        "da00_Attribute.h"
)

create_writer_module(da00)
