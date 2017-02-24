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

find_path(path_include_gtest NAMES gtest/gtest.h PATHS
$ENV{googletest_dir}
)
find_library(path_lib_gtest NAMES gtest PATHS
$ENV{googletest_dir}
)
if (path_lib_gtest)
add_library(libgtest SHARED IMPORTED)
set_property(TARGET libgtest PROPERTY IMPORTED_LOCATION ${path_lib_gtest})
endif()
set(have_gtest 0)
if (path_include_gtest AND path_lib_gtest)
set(have_gtest 1)
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


add_library( NeXusFileWriter
  Streamer.cxx
  StreamMaster.cxx
  kafka_util.cxx
  Master.cxx
  CommandListener.cxx
  NexusWriter.cxx
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
  date/tz.cpp
  date/tzblobs.cpp
  ${path_include_fmt}/fmt/format.cc
)
add_dependencies(NeXusFileWriter flatbuffers_generate)

target_include_directories(NeXusFileWriter PRIVATE
  ${path_include_fmt}
  ${path_include_hdf5}
  ${path_include_rdkafka}
  ${path_include_flatbuffers}
  ${path_include_rapidjson}
  ${CMAKE_CURRENT_BINARY_DIR}
  )


message(STATUS "ENV{streaming_data_types_dir}: $ENV{streaming_data_types_dir}")
find_path(path_streaming_data_types NAMES f140_general.fbs PATHS
  $ENV{streaming_data_types_dir}/schemas
  "${CMAKE_CURRENT_SOURCE_DIR}/../../streaming-data-types/schemas"
)
set(path_streaming_data_types ${path_streaming_data_types})
message(STATUS "path_streaming_data_types: ${path_streaming_data_types}")

target_compile_definitions(NeXusFileWriter PRIVATE HAS_REMOTE_API=0)


set(flatbuffers_generated_headers "")

# Search the dev env standard locations, but it will also look in PATH
find_program(flatc flatc HINTS $ENV{flatc_dir} $ENV{flatc} "/opt/local/flatbuffers")
message(STATUS "flatc in ${flatc}")

set(schemas_subdir "schemas")
set(head_out_dir "${CMAKE_CURRENT_BINARY_DIR}/${schemas_subdir}")
file(MAKE_DIRECTORY ${head_out_dir})
file(GLOB_RECURSE flatbuffers_schemata2 RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}/../../streaming-data-types/schemas" "${CMAKE_CURRENT_SOURCE_DIR}/../../streaming-data-types/schemas/*.fbs")

foreach (f0 ${flatbuffers_schemata2})
	string(REGEX REPLACE "\\.fbs$" "" s0 ${f0})
	#message(STATUS "s0: ${s0}")
	set(fbs "${s0}.fbs")
	set(fbh "${s0}_generated.h")
	message(STATUS "fbs: ${fbs}")
	#message(STATUS "fbh: ${fbh}")
	add_custom_command(
		OUTPUT "${head_out_dir}/${fbh}"
		COMMAND pwd && ${flatc} --cpp --gen-mutable --gen-name-strings --scoped-enums "../../../../streaming-data-types/schemas/${fbs}"
		DEPENDS "../../streaming-data-types/schemas/${fbs}"
		WORKING_DIRECTORY "${head_out_dir}"
		COMMENT "Process ${fbs} using ${flatc}"
	)
	list(APPEND flatbuffers_generated_headers "${head_out_dir}/${fbh}")
endforeach()

add_custom_target(flatbuffers_generate ALL DEPENDS ${flatbuffers_generated_headers})




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


set(tgt "send-command")
set(sources
send-command.cxx
KafkaW.cxx
logger.cxx
${path_include_fmt}/fmt/format.cc
${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx
)
add_executable(${tgt} ${sources})
add_dependencies(${tgt} git_commit_current)
target_include_directories(${tgt} PRIVATE
${path_include_fmt}
${path_include_rdkafka}
${path_include_rapidjson}
${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(${tgt} librdkafka)


set(tgt "kafka-to-nexus")
set(sources
kafka-to-nexus.cxx
schema_f141.cxx
${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx
)
if (have_gtest)
list(APPEND sources test-roundtrip.cxx)
list(APPEND sources ../test/HDFFile.cxx)
endif()
add_executable(${tgt} ${sources})

add_dependencies(${tgt} flatbuffers_generate)
add_dependencies(${tgt} git_commit_current)
add_dependencies(${tgt} NeXusFileWriter)

if (have_gtest)
message(STATUS "Using Google Test: ${path_include_gtest}  ${path_lib_gtest}")
target_compile_definitions(${tgt} PRIVATE HAVE_GTEST=1)
target_include_directories(${tgt} PRIVATE ${path_include_gtest})
target_link_libraries(${tgt} libgtest)
endif()

target_include_directories(${tgt} PRIVATE
${path_include_fmt}
${path_include_hdf5}
${path_include_rdkafka}
${path_include_flatbuffers}
${path_include_rapidjson}
${CMAKE_CURRENT_BINARY_DIR}
)


#if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
#  target_link_libraries(${tgt} hdf5_cpp)
#  target_link_libraries(${tgt} hdf5)
#  target_link_libraries(${tgt} rdkafka)
#  target_link_libraries(${tgt} rdkafka++)
#else()
  target_link_libraries(${tgt} libhdf5)
  target_link_libraries(${tgt} libhdf5cpp)
  target_link_libraries(${tgt} librdkafka)
  target_link_libraries(${tgt} librdkafka++)
#endif()

target_link_libraries(${tgt} NeXusFileWriter)

configure_file(../test/msg-conf-new-01.json ../test/msg-conf-new-01.json)
configure_file(../test/msg-conf-new-02.json ../test/msg-conf-new-02.json)
configure_file(../test/schema-command.json ../test/schema-command.json)
