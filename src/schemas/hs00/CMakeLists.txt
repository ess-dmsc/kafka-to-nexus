set(hs00_module_SRC
  Exceptions.cpp
  Reader.cpp
  Writer.cpp
  WriterTyped.cpp
  WriterUntyped.cpp
  Shape.cpp
  Slice.cpp
  HistogramRecord.cpp
  Dimension.cpp
)

set(hs00
_module_INC
  Exceptions.h
  Reader.h
  Writer.h
  WriterTyped.h
  WriterUntyped.h
  Shape.h
  Slice.h
  HistogramRecord.h
  Dimension.h
)

create_module(hs00_module)
