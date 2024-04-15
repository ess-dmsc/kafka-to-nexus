cmake_minimum_required(VERSION 3.10)
project("kafka-to-nexus")

set(CMAKE_CXX_STANDARD 17)

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug")
  message(STATUS "CMAKE_BUILD_TYPE was not specified so defaulting to DEBUG")
endif()

set(SRC_CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
list(APPEND CMAKE_MODULE_PATH ${SRC_CMAKE_MODULE_PATH})
list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR})

include(${SRC_CMAKE_MODULE_PATH}/Versioning.cmake)
set_version()
create_version_header(
  ${SRC_CMAKE_MODULE_PATH}/templates/Version.h.in
  ${PROJECT_SOURCE_DIR}/src/Version.h
)

# Extra warnings and treat warnings as errors
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wextra -Wnon-virtual-dtor -Wcast-align -Wunused -Woverloaded-virtual -Wpedantic -Wnull-dereference -Wdouble-promotion -Wformat=2")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -g -D_GLIBCXX_USE_NANOSLEEP")

if (APPLE)
  message(STATUS "Temporary suppressing deprecated warning (asio)")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated")
endif()

set(CONAN_PROFILE "default" CACHE STRING "Name of conan profile to use, uses default by default")
set(CONAN_FILE "conanfile.txt" CACHE STRING "Name of the conanfile to use, must be placed in the project's conan directory")
set(CONAN "AUTO" CACHE STRING "conan options AUTO (conan must be in path), MANUAL (expects conanbuildinfo.cmake in build directory) or DISABLE")

if(${CONAN} MATCHES "AUTO")
  include(${SRC_CMAKE_MODULE_PATH}/conan.cmake)
  conan_cmake_run(CONANFILE ${CONAN_FILE}
          PROFILE ${CONAN_PROFILE}
          BASIC_SETUP ${OUTPUT_DIRS}
          BUILD_TYPE "None"
          BUILD outdated
          KEEP_RPATHS)
elseif(${CONAN} MATCHES "MANUAL")
  if(EXISTS "${CMAKE_BINARY_DIR}/conanbuildinfo.cmake")
    include("${CMAKE_BINARY_DIR}/conanbuildinfo.cmake")
    conan_basic_setup(${OUTPUT_DIRS} KEEP_RPATHS)
  else()
    message(FATAL_ERROR "CONAN set to MANUAL but no file named conanbuildinfo.cmake found in build directory")
  endif()
elseif(${CONAN} MATCHES "DISABLE")
  message(WARNING "Conan is disabled so you will need to put your own cmake find modules in the project's \"cmake\" directory")
else()
  message(FATAL_ERROR "Unrecognised option for CONAN, use AUTO, MANUAL or DISABLE")
endif()

add_subdirectory(src)
add_subdirectory(apps)
add_subdirectory(tests)
