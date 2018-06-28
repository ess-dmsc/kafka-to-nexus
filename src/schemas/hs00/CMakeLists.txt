set(hs00_module_SRC
  Reader.cpp
  Writer.cpp
  WriterTyped.cpp
  Exceptions.cpp
  Shape.cpp
  Dimension.cpp
)

set(hs00_module_INC
  Reader.h
  Writer.h
  WriterTyped.cpp
  Exceptions.h
  Shape.h
  Dimension.cpp
)

create_module(hs00_module)
