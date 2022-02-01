cmake_minimum_required(VERSION 3.1.0)
project("kafka-to-nexus")

set(CMAKE_CXX_STANDARD 17)

set(SRC_CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
list(APPEND CMAKE_MODULE_PATH ${SRC_CMAKE_MODULE_PATH})
list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR})

include(${SRC_CMAKE_MODULE_PATH}/Versioning.cmake)
set_version()
set(VERSION_INCLUDE_DIR ${CMAKE_BINARY_DIR}/version)
create_version_header(
  ${SRC_CMAKE_MODULE_PATH}/templates/Version.h.in
  ${VERSION_INCLUDE_DIR}/Version.h
)

# Extra warnings and treat warnings as errors
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")

set(HTML_COVERAGE_REPORT OFF CACHE BOOL "Generate test coverage report as html, find in <BUILD_DIR>/coverage/index.html")
if(CMAKE_COMPILER_IS_GNUCXX AND COV OR HTML_COVERAGE_REPORT)
  include(CodeCoverage)
  append_coverage_compiler_flags()
  # Exclude tests and Metric sink implementations that do network IO from test coverage
  set(COVERAGE_EXCLUDE src/tests src/CLIOptions.cpp src/Metrics/Carbon* src/Metrics/Log* src/WriterModule/f142_test/)
  if (HTML_COVERAGE_REPORT)
    setup_target_for_coverage_gcovr_html(NAME coverage
                                         EXECUTABLE UnitTests
                                         EXCLUDE ${COVERAGE_EXCLUDE}
                                         DEPENDENCIES UnitTests)
  else()
    setup_target_for_coverage_gcovr_xml(NAME coverage
                                        EXECUTABLE UnitTests
                                        EXCLUDE ${COVERAGE_EXCLUDE}
                                        DEPENDENCIES UnitTests)
  endif()
endif()

# Use gold linker if available on system (it is faster than default GNU linker)
if (UNIX AND NOT APPLE)
  execute_process(COMMAND ${CMAKE_C_COMPILER} -fuse-ld=gold -Wl,--version ERROR_QUIET OUTPUT_VARIABLE ld_version)
  if ("${ld_version}" MATCHES "GNU gold")
    message(STATUS "Using gold linker")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
  endif()
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

set(SANITIZER "none" CACHE STRING "Sanitizer used to compile code with")
set_property(CACHE SANITIZER PROPERTY STRINGS none address thread)
if(SANITIZER STREQUAL "thread")
  message(STATUS "Thread sanitizer selected")
  set(SANITIZE_PARAMS "-fsanitize=thread")
elseif(SANITIZER STREQUAL "address")
  message(STATUS "Address sanitizer selected")
  set(SANITIZE_PARAMS "-fsanitize=address,undefined")
elseif(SANITIZER STREQUAL "none")
  message(STATUS "No sanitizer selected")
  set(SANITIZE_PARAMS "")
else()
  message(FATAL_ERROR "Unknown sanitizer (${SANITIZER}) selected.")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SANITIZE_PARAMS}")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SANITIZE_PARAMS}")
set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${SANITIZE_PARAMS}")

add_subdirectory(src)

option(RUN_DOXYGEN "Run doxygen" TRUE)
if (RUN_DOXYGEN)
configure_file(Doxygen.conf dg.conf)
add_custom_target(docs COMMAND doxygen "${PROJECT_BINARY_DIR}/dg.conf" WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
endif()
