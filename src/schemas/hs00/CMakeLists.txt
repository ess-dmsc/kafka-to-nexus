set(hs00_module_SRC
  Reader.cpp
  Writer.cpp
  WriterUntyped.cpp
  WriterTyped.cpp
  Shape.cpp
  Dimension.cpp
  Slice.cpp
  HistogramRecord.cpp
)

set(hs00_module_INC
  Reader.h
  Writer.h
  WriterUntyped.h
  WriterTyped.h
  Exceptions.h
  Shape.h
  Dimension.h
  Slice.h
  HistogramRecord.h
)

create_module(hs00_module)
