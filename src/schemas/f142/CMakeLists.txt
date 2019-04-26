set(f142_module_SRC
  f142_rw.cpp
  FlatbufferReader.cpp
  WriterScalarString.cpp
)

set(f142_module_INC
  Common.h
  f142_rw.h
  FlatbufferReader.h
  WriterArray.h
  WriterScalar.h
  WriterScalarString.h
  WriterTypedBase.h
)

create_module(f142_module)
