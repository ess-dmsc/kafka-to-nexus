project("ess-file-writer")

# Relative to build root
#set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ..)

if (NOT CMAKE_BUILD_TYPE)
set(CMAKE_BUILD_TYPE Debug)
endif()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)
set (CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib)

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
  "$ENV{fmt_dir}/include"
  /opt/local
)

find_path(path_include_gtest NAMES gtest/gtest.h)
find_library(path_lib_gtest NAMES gtest)
if (path_lib_gtest)
add_library(libgtest SHARED IMPORTED)
set_property(TARGET libgtest PROPERTY IMPORTED_LOCATION ${path_lib_gtest})
endif()

find_path(path_include_flatbuffers NAMES flatbuffers/flatbuffers.h PATHS
 "$ENV{flatbuffers_dir}/include"
 /opt/local/flatbuffers.git/include
)

find_path(path_include_rapidjson NAMES rapidjson/document.h PATHS
 "$ENV{rapidjson_dir}/include"
 /opt/local/rapidjson.git/include
)

find_path(path_include_hdf5 NAMES hdf5.h PATHS
  "$ENV{hdf5_dir}/include"
)

find_library(path_lib_hdf5 NAMES hdf5-shared PATHS
  "$ENV{hdf5_dir}/lib"
  )
find_library(path_lib_hdf5cpp NAMES hdf5_cpp-shared PATHS
  "$ENV{hdf5_dir}/lib"
  )
if( NOT path_lib_hdf5 )
  find_library(path_lib_hdf5 NAMES hdf5 PATHS
  "$ENV{hdf5_dir}/lib"
  )
  find_library(path_lib_hdf5cpp NAMES hdf5_cpp PATHS
    "$ENV{hdf5_dir}/lib"
    )
endif()

add_library(libhdf5 SHARED IMPORTED)
set_property(TARGET libhdf5 PROPERTY IMPORTED_LOCATION ${path_lib_hdf5})

add_library(libhdf5cpp SHARED IMPORTED)
set_property(TARGET libhdf5cpp PROPERTY IMPORTED_LOCATION ${path_lib_hdf5cpp})

find_path(path_include_rdkafka NAMES librdkafka/rdkafka.h PATHS
 "$ENV{librdkafka_dir}/include"
 /opt/local/rdkafka/install/include
 /usr/local/include
)

find_library(path_lib_rdkafka++ NAMES rdkafka++ PATHS
 "$ENV{librdkafka_dir}/lib"
 /opt/local/rdkafka/install/lib
 /usr/local/lib
)
add_library(librdkafka++ SHARED IMPORTED)
set_property(TARGET librdkafka++ PROPERTY IMPORTED_LOCATION ${path_lib_rdkafka++})

find_path(path_streaming_data_types NAMES f140-general.fbs PATHS
../../streaming-data-types
)

find_path(path_flatc NAMES flatc PATHS
$ENV{flatc_dir}
)


#add_library( rdkafka_mock KafkaMock.cxx)
add_library( NeXusFileWriter Streamer.cxx StreamMaster.cxx DemuxTopic.cxx TimeDifferenceFromMessage.cxx)
target_include_directories(NeXusFileWriter PRIVATE
${path_include_fmt}
${path_include_hdf5}
${path_include_rdkafka}
${path_include_flatbuffers}
${path_include_rapidjson}
${CMAKE_CURRENT_BINARY_DIR}
)



# TODO
# Import the actual schemes
set(flatbuffers_schemata
f140-general
f141-ntarraydouble
)
set(flatbuffers_generated_headers "")

foreach (f ${flatbuffers_schemata})
	set(fbs "${path_streaming_data_types}/${f}.fbs")
	set(fbh "${f}_generated.h")
	add_custom_command(
		OUTPUT ${fbh}
		COMMAND "${path_flatc}/flatc" --cpp "${fbs}"
		DEPENDS "${fbs}"
		COMMENT "Process ${fbs} using ${flatc}"
	)
	list(APPEND flatbuffers_generated_headers "${fbh}")
endforeach()

add_custom_target(flatbuffers_generate ALL DEPENDS ${flatbuffers_generated_headers})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -Wall -fPIC -g -D_GLIBCXX_USE_NANOSLEEP")
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


set(tgt ess-file-writer)

add_executable(${tgt}
ess-file-writer.cxx
kafka_util.cxx
Master.cxx
CommandListener.cxx
NexusWriter.cxx
TimeDifferenceFromMessage.cxx
FileWriterTask.cxx
Source.cxx
DemuxTopic.cxx
HDFFile.cxx
helper.cxx
logger.cxx
json.cxx
${path_include_fmt}/fmt/format.cc
${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx
)
set_target_properties(${tgt} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)

add_dependencies(${tgt} flatbuffers_generate)
add_dependencies(${tgt} git_commit_current)

if (path_include_gtest AND path_lib_gtest)
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

#target_link_libraries(${tgt} libhdf5)
target_link_libraries(${tgt} libhdf5cpp)
target_link_libraries(${tgt} librdkafka++)

configure_file(../test/msg-conf-new-01.json ../test/msg-conf-new-01.json)
configure_file(../test/msg-conf-new-02.json ../test/msg-conf-new-02.json)
configure_file(../test/schema-command.json ../test/schema-command.json)
