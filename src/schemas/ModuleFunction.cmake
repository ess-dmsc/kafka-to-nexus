#=============================================================================
# Generate file-writer module
# Note that you will still need to manually include the code in the
# 'kafka-to-nexus' target as well as the 'UnitTests' target
# (see src/CMakeLists.txt for examples).
#=============================================================================
function(create_module module_name)
  add_library(${module_name} OBJECT
    ${${module_name}_SRC}
    ${${module_name}_INC}
  )
  target_include_directories(${module_name} PRIVATE ${path_include_common} ..)
  set(WRITER_MODULES ${WRITER_MODULES} $<TARGET_OBJECTS:${module_name}> CACHE INTERNAL "WRITER_MODULES")
endfunction(create_module)
