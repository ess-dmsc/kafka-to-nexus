set(TEST_SOURCE_DIR ${PROJECT_SOURCE_DIR}/test)
set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set (CMAKE_CXX_FLAGS "-std=c++1y -D_GLIBCXX_USE_NANOSLEEP ${CMAKE_CXX_FLAGS} -ggdb")

find_path(path_include_rapidjson NAMES rapidjson/document.h PATHS
 "$ENV{rapidjson_dir}/include"
 /opt/local/rapidjson.git/include
)

find_path(path_include_hdf5 NAMES hdf5.h HINTS
"$ENV{hdf5_dir}/include"
)
message(STATUS "path_include_hdf5" ${path_include_hdf5})

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

find_library(path_lib_rdkafka NAMES rdkafka PATHS
"$ENV{librdkafka_dir}/lib"
/opt/local/rdkafka/install/lib
/usr/local/lib
)
add_library(librdkafka SHARED IMPORTED)
set_property(TARGET librdkafka PROPERTY IMPORTED_LOCATION ${path_lib_rdkafka})

find_library(path_lib_rdkafka++ NAMES rdkafka++ PATHS
 "$ENV{librdkafka_dir}/lib"
 /opt/local/rdkafka/install/lib
 /usr/local/lib
 )
add_library(librdkafka++ SHARED IMPORTED)
set_property(TARGET librdkafka++ PROPERTY IMPORTED_LOCATION ${path_lib_rdkafka++})


include_directories (
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/src
  ${TEST_SOURCE_DIR}
  ${path_include_rapidjson}
  ${path_include_rdkafka}
  ${path_include_hdf5}
  ) 


add_executable (streamer_test streamer_test.cxx)
add_executable (streammaster_test streammaster_test.cxx ) 

find_path(path_include_gtest NAMES gtest/gtest.h PATHS
$ENV{googletest_dir}
)

find_library(path_lib_gtest NAMES gtest PATHS
  $ENV{googletest_dir}
  )
add_library(libgtest SHARED IMPORTED)
set_property(TARGET libgtest PROPERTY IMPORTED_LOCATION ${path_lib_gtest})
add_library(libgtest_main SHARED IMPORTED)
set_property(TARGET libgtest_main PROPERTY IMPORTED_LOCATION ${path_lib_gtest})

target_link_libraries(streammaster_test libhdf5cpp
  libhdf5
  librdkafka
  librdkafka++
  libgtest
  libgtest_main
  NeXusFileWriter
  )

target_link_libraries(streamer_test libhdf5cpp
  libhdf5
  librdkafka
  librdkafka++
  libgtest
  libgtest_main
  NeXusFileWriter
  )

