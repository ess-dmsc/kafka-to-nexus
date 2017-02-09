cmake_minimum_required(VERSION 2.8.11)
add_subdirectory(src)
add_subdirectory(test)

option(RUN_DOXYGEN "Run doxygen" TRUE)

if (RUN_DOXYGEN)
configure_file(Doxygen.conf dg.conf)
add_custom_target(docs COMMAND doxygen dg.conf)
endif()

include( CTest )

enable_testing()
add_custom_target(build_and_test ${CMAKE_CTEST_COMMAND} -V)
add_test(NAME streamer_test WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/streamer_test)
add_test(NAME streammaster_test WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/streammaster_test)
