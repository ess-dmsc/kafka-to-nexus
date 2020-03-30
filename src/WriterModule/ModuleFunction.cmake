#=============================================================================
# Generate file-writer module
# Note that you will still need to manually include the code in the
# 'kafka-to-nexus' target as well as the 'UnitTests' target
# (see src/CMakeLists.txt for examples).
#=============================================================================
function(create_writer_module module_name)
  set(used_name ${module_name}_writer)
  add_library(${used_name} OBJECT
    ${${module_name}_SRC}
    ${${module_name}_INC}
  )
  target_include_directories(${used_name} PRIVATE ${path_include_common} ..)
  set(WRITER_MODULES ${WRITER_MODULES} $<TARGET_OBJECTS:${used_name}> CACHE INTERNAL "WRITER_MODULES")
endfunction()
