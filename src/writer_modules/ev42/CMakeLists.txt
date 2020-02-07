set(ev42_SRC
  ev42_Writer.cpp
  AdcDatasets.cpp
)

set(ev42_INC
    ev42_Writer.h
    AdcDatasets.h
)

create_writer_module(ev42)
