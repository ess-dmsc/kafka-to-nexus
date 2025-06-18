cmake_minimum_required(VERSION 3.21)
project("kafka-to-nexus")

set(CMAKE_CXX_STANDARD 17)

list(APPEND CMAKE_PREFIX_PATH "${CMAKE_BINARY_DIR}/generators")

# Set output directories
include(GNUInstallDirs)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Add Conan dependencies
find_package(asio REQUIRED)
find_package(spdlog REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(RdKafka REQUIRED)
find_package(readerwriterqueue REQUIRED)
find_package(HDF5 REQUIRED)
find_package(h5cpp REQUIRED)
find_package(concurrentqueue REQUIRED)
find_package(date REQUIRED)
find_package(flatbuffers REQUIRED)
find_package(streaming-data-types REQUIRED)
find_package(stduuid REQUIRED)
find_package(spdlog REQUIRED)

# Common dependency group
set(COMMON_LIBS
    asio::asio
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    RdKafka::rdkafka
    RdKafka::rdkafka++
    readerwriterqueue::readerwriterqueue
    HDF5::HDF5
    h5cpp::h5cpp
    concurrentqueue::concurrentqueue
    date::date
    flatbuffers::flatbuffers_shared
    streaming-data-types::streaming-data-types
    stduuid::stduuid
)

set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
if(APPLE)
    set(CMAKE_INSTALL_RPATH "@executable_path/../lib")
elseif(UNIX)
    set(CMAKE_INSTALL_RPATH "\$ORIGIN/../lib")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

include("${CMAKE_SOURCE_DIR}/cmake/Versioning.cmake")

set_version()
create_version_header(
  "${CMAKE_SOURCE_DIR}/cmake/templates/Version.h.in"
  "${CMAKE_SOURCE_DIR}/src/Version.h"
)

# Extra warnings and treat warnings as errors
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wextra -Wnon-virtual-dtor -Wcast-align -Wunused -Woverloaded-virtual -Wdouble-promotion -Wformat=2")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -g -D_GLIBCXX_USE_NANOSLEEP")

if (APPLE)
  message(STATUS "Temporary suppressing deprecated warning (asio)")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated")
endif()

option(COV "Enable code coverage reporting with gcov/gcovr" OFF)
if(COV)
    find_program(GCOVR_PATH gcovr REQUIRED)

    add_custom_target(coverage
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/coverage
        COMMAND ${GCOVR_PATH} -r ${CMAKE_SOURCE_DIR}
            --gcov-executable gcov
            --filter ${CMAKE_SOURCE_DIR}/src
            --exclude ${CMAKE_SOURCE_DIR}/apps
            --exclude ${CMAKE_SOURCE_DIR}/tests
            --exclude ${CMAKE_SOURCE_DIR}/.conan
            --exclude-unreachable-branches
            --exclude-throw-branches
            --print-summary
            --xml ${CMAKE_BINARY_DIR}/coverage/coverage.xml
            --xml-pretty
            --html ${CMAKE_BINARY_DIR}/coverage/coverage.html
            --html-details ${CMAKE_BINARY_DIR}/coverage/
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating code coverage report in coverage/ using gcovr"
    )
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

set(SANITIZER "none" CACHE STRING "Sanitizer used to compile code with")
set_property(CACHE SANITIZER PROPERTY STRINGS none address thread)
if(SANITIZER STREQUAL "none")
elseif(SANITIZER STREQUAL "thread")
  message(STATUS "Thread sanitizer selected")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -g")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread -g")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=thread -g")
elseif(SANITIZER STREQUAL "address")
  message(STATUS "Address sanitizer selected")
  set(SANITIZE_PARAMS "-fsanitize=address,undefined -fno-omit-frame-pointer -g")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address,undefined -fno-omit-frame-pointer -g")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address,undefined -fno-omit-frame-pointer -g")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=address,undefined -fno-omit-frame-pointer -g")
else()
  message(FATAL_ERROR "Unknown sanitizer (${SANITIZER}) selected.")
endif()

add_subdirectory(src)
add_subdirectory(apps)

enable_testing()
add_subdirectory(tests)

option(RUN_DOXYGEN "Run doxygen" ON)
if(RUN_DOXYGEN)
    find_package(Doxygen REQUIRED)

    set(DOXYGEN_INPUT_DIR "${CMAKE_SOURCE_DIR}/src")
    set(DOXYGEN_OUTPUT_DIR "${CMAKE_BINARY_DIR}/docs")

    set(DOXYGEN_CONFIG_FILE_IN "${CMAKE_SOURCE_DIR}/Doxygen.conf")
    set(DOXYGEN_CONFIG_FILE_OUT "${CMAKE_BINARY_DIR}/dg.conf")

    configure_file(${DOXYGEN_CONFIG_FILE_IN} ${DOXYGEN_CONFIG_FILE_OUT} @ONLY)

    add_custom_target(docs
        COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_CONFIG_FILE_OUT}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM
    )
endif()
