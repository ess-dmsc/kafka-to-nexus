# Locates the streaming-data-types repository and sets up a target
# called 'flatbuffers_generate' which generates the flatbuffer headers.
# Make your target depend on 'flatbuffers_generate'.
# When you call this using find_package, you can specify a minimum version
# for the streaming data types:
# find_package(streaming_data_types COMPONENTS <minimum-git-commit>)
# you must then make your target depend on 'check_streaming_data_types'
# so that it can abort compilation if streaming data types does not suffice.
# Usage of this package requires of course the git suite to be available.
# Contact: Dominik Werder

find_path(path_include_streaming_data_types NAMES schemas/f141_epics_nt.fbs HINTS
# The common case as fallback:
${CMAKE_CURRENT_SOURCE_DIR}/../../streaming-data-types
)
if (NOT path_include_streaming_data_types)
	find_path(path_include_streaming_data_types NAMES schemas/f141_epics_nt.fbs HINTS
			  # Also try with the common .git suffix:
			  ${CMAKE_CURRENT_SOURCE_DIR}/../../streaming-data-types.git
			  )
	if (NOT path_include_streaming_data_types)
		message(FATAL_ERROR "Can not find_path() the streaming-data-types repository")
	endif()
endif()
message(STATUS "path_include_streaming_data_types ${path_include_streaming_data_types}")

if (NOT StreamingDataTypes_FIND_COMPONENTS STREQUAL "")
add_custom_target(check_streaming_data_types ALL
COMMAND bash -c '\(cd ${path_include_streaming_data_types} && git merge-base --is-ancestor ${StreamingDataTypes_FIND_COMPONENTS} HEAD \) || \( echo && echo ERROR\ Your\ streaming-data-types\ repository\ is\ too\ old\ we\ require\ at\ least\ ${StreamingDataTypes_FIND_COMPONENTS} && echo && exit 1 \) '
)
endif()



set(flatbuffers_generated_headers "")

set(schemas_subdir "schemas")
set(head_out_dir "${CMAKE_CURRENT_BINARY_DIR}/${schemas_subdir}")
file(MAKE_DIRECTORY ${head_out_dir})
file(GLOB_RECURSE flatbuffers_schemata2 RELATIVE "${path_include_streaming_data_types}/schemas" "${path_include_streaming_data_types}/schemas/*.fbs")

foreach (f0 ${flatbuffers_schemata2})
	string(REGEX REPLACE "\\.fbs$" "" s0 ${f0})
	set(fbs "${s0}.fbs")
	set(fbh "${s0}_generated.h")
	add_custom_command(
		OUTPUT "${head_out_dir}/${fbh}"
		COMMAND ${FLATBUFFERS_FLATC_EXECUTABLE} --cpp --gen-mutable --gen-name-strings --scoped-enums "${path_include_streaming_data_types}/schemas/${fbs}"
		DEPENDS "${path_include_streaming_data_types}/schemas/${fbs}"
		WORKING_DIRECTORY "${head_out_dir}"
		COMMENT "Process ${fbs} using ${flatc}"
	)
	list(APPEND flatbuffers_generated_headers "${head_out_dir}/${fbh}")
endforeach()

add_custom_target(flatbuffers_generate ALL DEPENDS ${flatbuffers_generated_headers})
if (DEFINED check_streaming_data_types)
	add_dependencies(flatbuffers_generate check_streaming_data_types)
endif()
