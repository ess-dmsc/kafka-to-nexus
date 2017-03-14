cmake_minimum_required(VERSION 2.8.11)

find_package(HDF5 COMPONENTS CXX REQUIRED)

add_subdirectory(src)

option(RUN_DOXYGEN "Run doxygen" TRUE)
if (RUN_DOXYGEN)
configure_file(Doxygen.conf dg.conf)
add_custom_target(docs COMMAND doxygen dg.conf)
endif()
