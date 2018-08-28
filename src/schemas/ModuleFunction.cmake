#=============================================================================
# Generate file-writer module
# Note that you will still need to manually include the code the
# 'kafka_to_nexus__objects' target.
#=============================================================================
function(create_module module_name)
  add_library(${module_name} OBJECT
    ${${module_name}_SRC}
    ${${module_name}_INC}
  )
  target_include_directories(${module_name} PRIVATE ${path_include_common})
endfunction(create_module)
