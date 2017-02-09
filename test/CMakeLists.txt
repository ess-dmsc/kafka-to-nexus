set(TEST_SOURCE_DIR ${PROJECT_SOURCE_DIR}/test)
set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
set (CMAKE_CXX_FLAGS "-std=c++1y -D_GLIBCXX_USE_NANOSLEEP ${CMAKE_CXX_FLAGS} -ggdb")

find_path(path_include_rapidjson NAMES rapidjson/document.h PATHS
 "$ENV{rapidjson_dir}/include"
 /opt/local/rapidjson.git/include
)

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


include_directories (
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/src
  ${TEST_SOURCE_DIR}
  ${path_include_rapidjson}
  ${path_include_rdkafka}
  ) 

add_executable (streamer_test streamer_test.cxx) 
add_executable (streammaster_test streammaster_test.cxx ) 


find_path(path_include_gtest NAMES gtest/gtest.h PATHS
  "$ENV{googletest_dir}/include"
  )

find_path(path_lib_gtest NAMES gtest PATHS "$ENV{googletest_dir}/lib")

if (path_lib_gtest)
  add_library(libgtest SHARED IMPORTED)
  set_property(TARGET libgtest PROPERTY IMPORTED_LOCATION ${path_lib_gtest})
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries(streammaster_test hdf5_cpp)
  target_link_libraries(streammaster_test hdf5)
  target_link_libraries(streammaster_test rdkafka)
  target_link_libraries(streammaster_test rdkafka++)
  set(GTEST_BOTH_LIBRARIES gtest gtest_main)
else()
  target_link_libraries(streammaster_test libhdf5)
  target_link_libraries(streammaster_test librdkafka++)
  set(GTEST_BOTH_LIBRARIES libgtest libgtest_main)
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries(streamer_test hdf5_cpp)
  target_link_libraries(streamer_test hdf5)
  target_link_libraries(streamer_test rdkafka)
  target_link_libraries(streamer_test rdkafka++)
else()
  target_link_libraries(streamer_test libhdf5)
  target_link_libraries(streamer_test librdkafka++)
endif()

target_link_libraries( streamer_test
		       ${GTEST_BOTH_LIBRARIES}
                       NeXusFileWriter)


                     
target_link_libraries( streammaster_test
		       ${GTEST_BOTH_LIBRARIES}
                       NeXusFileWriter)

add_test(NAME streamer_test WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/streamer_test)
add_test(NAME streammaster_test WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/streammaster_test)
