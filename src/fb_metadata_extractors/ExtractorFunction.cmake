#=============================================================================
# Generate file-writer module
# Note that you will still need to manually include the code in the
# 'kafka-to-nexus' target as well as the 'UnitTests' target
# (see src/CMakeLists.txt for examples).
#=============================================================================
function(create_fb_extractor module_name)
  SET(used_name ${module_name}_extractor)
  add_library(${used_name} OBJECT
    ${${module_name}_SRC}
    ${${module_name}_INC}
  )
  target_include_directories(${used_name} PRIVATE ${path_include_common} ..)
  set(FB_METADATA_EXTRACTORS ${FB_METADATA_EXTRACTORS} $<TARGET_OBJECTS:${used_name}> CACHE INTERNAL "FB_METADATA_EXTRACTORS")
endfunction()
