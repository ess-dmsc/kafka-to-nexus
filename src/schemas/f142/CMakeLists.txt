set(f142_module_SRC
  f142_rw.cpp
  f142_synth.cpp
  FlatbufferReader.cpp
  WriterScalarString.cpp
)

set(f142_module_INC
  Common.h
  f142_rw.h
  f142_synth.h
  FlatbufferReader.h
  WriterArray.h
  WriterScalar.h
  WriterScalarString.h
  WriterTypedBase.h
)

create_module(f142_module)
