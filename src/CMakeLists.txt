project("kafka-to-nexus")

# Relative to build root
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ..)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ..)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ..)

if (NOT CMAKE_BUILD_TYPE)
set(CMAKE_BUILD_TYPE Debug)
endif()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx "extern \"C\" char const GIT_COMMIT[] = \"NOTSET\";\n")

add_custom_target(git_commit_now ALL
COMMAND echo 'extern \"C\" char const GIT_COMMIT[] = \"'`git rev-parse HEAD`'\"\;' > ${CMAKE_CURRENT_BINARY_DIR}/git_commit_now
WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_custom_target(git_commit_current ALL
COMMAND git diff --name-only --no-index ${CMAKE_CURRENT_BINARY_DIR}/git_commit_now ${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx || cp ${CMAKE_CURRENT_BINARY_DIR}/git_commit_now ${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx
DEPENDS git_commit_now
)

find_path(path_include_fmt NAMES fmt/format.cc PATHS
$ENV{fmt_dir}/include
/opt/local/fmt
# For DMSC build node:
$ENV{DM_ROOT}/usr/include
/opt/local
)

set(have_gtest 0)
find_path(path_include_gtest NAMES gtest/gtest.h PATHS
$ENV{googletest_dir}
)
find_library(path_lib_gtest NAMES gtest PATHS
$ENV{googletest_dir}
)
if (path_include_gtest AND path_lib_gtest)
set(have_gtest 1)
message(STATUS "Using Google Test: ${path_include_gtest}  ${path_lib_gtest}")
add_library(libgtest SHARED IMPORTED)
set_property(TARGET libgtest PROPERTY IMPORTED_LOCATION ${path_lib_gtest})
else()
message(STATUS "Not found: gtest, not using it.")
endif()


find_path(path_include_flatbuffers NAMES flatbuffers/flatbuffers.h HINTS
"$ENV{flatbuffers_dir}/include"
/opt/local/flatbuffers.git/include
)

find_path(path_include_rapidjson NAMES rapidjson/document.h HINTS
"$ENV{rapidjson_dir}/include"
/opt/local/rapidjson.git/include
)

find_path(path_include_hdf5 NAMES hdf5.h HINTS
"$ENV{hdf5_dir}/include"
)

find_library(path_lib_hdf5 NAMES hdf5-shared HINTS
"$ENV{hdf5_dir}/lib"
)
find_library(path_lib_hdf5cpp NAMES hdf5_cpp-shared HINTS
"$ENV{hdf5_dir}/lib"
)
if( NOT path_lib_hdf5 )
  find_library(path_lib_hdf5 NAMES hdf5 HINTS
  "$ENV{hdf5_dir}/lib"
  )
  find_library(path_lib_hdf5cpp NAMES hdf5_cpp HINTS
    "$ENV{hdf5_dir}/lib"
    )
endif()


add_library(libhdf5 SHARED IMPORTED)
set_property(TARGET libhdf5 PROPERTY IMPORTED_LOCATION ${path_lib_hdf5})

add_library(libhdf5cpp SHARED IMPORTED)
set_property(TARGET libhdf5cpp PROPERTY IMPORTED_LOCATION ${path_lib_hdf5cpp})

find_path(path_include_rdkafka NAMES librdkafka/rdkafka.h HINTS
"$ENV{librdkafka_dir}/include"
/opt/local/rdkafka/install/include
PATHS
/usr/local/include
)

find_library(path_lib_rdkafka NAMES rdkafka HINTS
"$ENV{librdkafka_dir}/lib"
/opt/local/rdkafka/install/lib
PATHS
/usr/local/lib
)
add_library(librdkafka SHARED IMPORTED)
set_property(TARGET librdkafka PROPERTY IMPORTED_LOCATION ${path_lib_rdkafka})

find_library(path_lib_rdkafka++ NAMES rdkafka++ HINTS
"$ENV{librdkafka_dir}/lib"
/opt/local/rdkafka/install/lib
/usr/local/lib
)
add_library(librdkafka++ SHARED IMPORTED)
set_property(TARGET librdkafka++ PROPERTY IMPORTED_LOCATION ${path_lib_rdkafka++})

find_path(path_include_graylog_logger NAMES graylog_logger/Log.hpp PATHS
$ENV{graylog_logger_dir}/include
/opt/local/graylog_logger-install
)
find_library(path_lib_graylog_logger NAMES graylog_logger PATHS
$ENV{graylog_logger_dir}/lib
/opt/local/graylog_logger-install
)
if (path_include_graylog_logger AND path_lib_graylog_logger)
message(STATUS "Using graylog_logger ${path_include_graylog_logger} ${path_lib_graylog_logger}")
set(have_graylog_logger TRUE)
add_library(libgraylog_logger SHARED IMPORTED)
set_property(TARGET libgraylog_logger PROPERTY IMPORTED_LOCATION ${path_lib_graylog_logger})
else()
message(STATUS "Not found: graylog_logger, not using it.")
endif()


message(STATUS "ENV{streaming_data_types_dir}: $ENV{streaming_data_types_dir}")
find_path(path_include_streaming_data_types NAMES schemas/f141_epics_nt.fbs PATHS
$ENV{streaming_data_types_dir}
"${CMAKE_CURRENT_SOURCE_DIR}/../../streaming-data-types"
)
message(STATUS "path_include_streaming_data_types: ${path_include_streaming_data_types}")


# Minimum commit of streaming-data-types that we need:
set(need_commit d2247ffd906ebeeb68c9b8a2ea9f1d36c1d88a46)

add_custom_target(check_streaming_data_types ALL
COMMAND bash -c '\(cd ${path_include_streaming_data_types} && git merge-base --is-ancestor ${need_commit} HEAD \) || \( echo ERROR\ Your\ streaming-data-types\ repository\ is\ too\ old && exit 1 \) '
)


set(flatbuffers_generated_headers "")

# Search the dev env standard locations, but it will also look in PATH
find_program(flatc flatc PATHS "$ENV{flatc}" "$ENV{flatc_dir}" "/opt/local/flatbuffers")
message(STATUS "flatc in ${flatc}")

set(schemas_subdir "schemas")
set(head_out_dir "${CMAKE_CURRENT_BINARY_DIR}/${schemas_subdir}")
file(MAKE_DIRECTORY ${head_out_dir})
file(GLOB_RECURSE flatbuffers_schemata2 RELATIVE "${path_include_streaming_data_types}/schemas" "${path_include_streaming_data_types}/schemas/*.fbs")

foreach (f0 ${flatbuffers_schemata2})
	string(REGEX REPLACE "\\.fbs$" "" s0 ${f0})
	#message(STATUS "s0: ${s0}")
	set(fbs "${s0}.fbs")
	set(fbh "${s0}_generated.h")
	#message(STATUS "fbs: ${fbs}")
	#message(STATUS "fbh: ${fbh}")
	add_custom_command(
		OUTPUT "${head_out_dir}/${fbh}"
		COMMAND ${flatc} --cpp --gen-mutable --gen-name-strings --scoped-enums "${path_include_streaming_data_types}/schemas/${fbs}"
		DEPENDS "${path_include_streaming_data_types}/schemas/${fbs}"
		WORKING_DIRECTORY "${head_out_dir}"
		COMMENT "Process ${fbs} using ${flatc}"
	)
	list(APPEND flatbuffers_generated_headers "${head_out_dir}/${fbh}")
endforeach()

add_custom_target(flatbuffers_generate ALL DEPENDS ${flatbuffers_generated_headers})
add_dependencies(flatbuffers_generate check_streaming_data_types)




set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -fPIC -g -D_GLIBCXX_USE_NANOSLEEP")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11 -O0 -fno-inline -ggdb -D_GLIBCXX_USE_NANOSLEEP")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++11 -O3 -D_GLIBCXX_USE_NANOSLEEP")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wpedantic -Wno-variadic-macros")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fomit-frame-pointer")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline -fno-omit-frame-pointer")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")

if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.9.3")
else ()
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=auto")
endif()


add_library(kafka_to_nexus__objects OBJECT
Streamer.cxx
StreamMaster.cxx
kafka_util.cxx
Master.cxx
CommandListener.cxx
CommandHandler.cxx
TimeDifferenceFromMessage.cxx
FileWriterTask.cxx
Source.cxx
DemuxTopic.cxx
HDFFile.cxx
SchemaRegistry.cxx
KafkaW.cxx
helper.cxx
logger.cxx
json.cxx
uri.cxx
date/tz.cpp
date/tzblobs.cpp
schema_f141.cxx
schemas/f141/synth.cxx
schemas/ev42/ev42_rw.cxx
schemas/ev42/ev42_synth.cxx
schemas/events/schema_amo0.cxx
MainOpt.cxx
${path_include_fmt}/fmt/format.cc
${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx
)
target_include_directories(kafka_to_nexus__objects PRIVATE
${path_include_fmt}
${path_include_hdf5}
${path_include_rdkafka}
${path_include_flatbuffers}
${path_include_rapidjson}
${CMAKE_CURRENT_BINARY_DIR}
)
target_compile_definitions(kafka_to_nexus__objects PRIVATE HAS_REMOTE_API=0)
add_dependencies(kafka_to_nexus__objects flatbuffers_generate)
if (have_gtest)
target_compile_definitions(kafka_to_nexus__objects PRIVATE HAVE_GTEST=1)
target_include_directories(kafka_to_nexus__objects PRIVATE ${path_include_gtest})
endif()



set(tgt "send-command")
set(sources
send-command.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} git_commit_current)
target_include_directories(${tgt} PRIVATE
${path_include_fmt}
${path_include_rdkafka}
${path_include_rapidjson}
)
target_link_libraries(${tgt} libhdf5)
target_link_libraries(${tgt} librdkafka)
target_link_libraries(${tgt} librdkafka++)
target_link_libraries(${tgt} pcre2-8)
if (have_gtest)
target_link_libraries(${tgt} libgtest)
endif()



set(tgt "kafka-to-nexus")
set(sources
kafka-to-nexus.cxx
$<TARGET_OBJECTS:kafka_to_nexus__objects>
)
add_executable(${tgt} ${sources})

add_dependencies(${tgt} flatbuffers_generate)
add_dependencies(${tgt} git_commit_current)

target_include_directories(${tgt} PRIVATE
${path_include_fmt}
${path_include_hdf5}
${path_include_rdkafka}
${path_include_flatbuffers}
${path_include_rapidjson}
)

target_link_libraries(${tgt} libhdf5)
target_link_libraries(${tgt} libhdf5cpp)
target_link_libraries(${tgt} librdkafka)
target_link_libraries(${tgt} librdkafka++)
target_link_libraries(${tgt} pcre2-8)

if (have_gtest)
target_link_libraries(${tgt} libgtest)
endif()

if (have_graylog_logger)
target_compile_definitions(${tgt} PRIVATE HAVE_GRAYLOG_LOGGER=1)
target_include_directories(${tgt} PRIVATE ${path_include_graylog_logger})
target_link_libraries(${tgt} libgraylog_logger)
endif()

if (have_gtest)
add_subdirectory(tests)
endif()
