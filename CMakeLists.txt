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

include(Versioning)
include(CodeCoverage)

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
