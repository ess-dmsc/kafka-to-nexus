cmake_minimum_required(VERSION 2.8.11)
project("kafka-to-nexus")

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_SOURCE_DIR}/cmake")

# Get googletest
if(BUILD_TESTING)
    # Download and  build gtest and gmock
    if (CMAKE_VERSION VERSION_LESS 3.2)
        set(UPDATE_DISCONNECTED_IF_AVAILABLE "")
    else()
        set(UPDATE_DISCONNECTED_IF_AVAILABLE "UPDATE_DISCONNECTED 1")
    endif()

    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/DownloadProject.cmake)
    download_project(PROJ                googletest
                     GIT_REPOSITORY      https://github.com/google/googletest.git
                     GIT_TAG             release-1.8.0
                     ${UPDATE_DISCONNECTED_IF_AVAILABLE})

    # Prevent GoogleTest from overriding our compiler/linker options
    # when building with Visual Studio
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

    add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})

    # When using CMake 2.8.11 or later, header path dependencies
    # are automatically added to the gtest and gmock targets.
    # For earlier CMake versions, we have to explicitly add the
    # required directories to the header search path ourselves.
    if (CMAKE_VERSION VERSION_LESS 2.8.11)
        include_directories("${gtest_SOURCE_DIR}/include"
                "${gmock_SOURCE_DIR}/include")
    endif()
    function(add_gtest_to_target tgt)
        add_dependencies(${tgt} gtest)
        target_link_libraries(${tgt} gtest gmock)
    endfunction()
endif(BUILD_TESTING)

##### Get libfmt ######
if (CMAKE_VERSION VERSION_LESS 3.2)
    set(UPDATE_DISCONNECTED_IF_AVAILABLE "")
else()
    set(UPDATE_DISCONNECTED_IF_AVAILABLE "UPDATE_DISCONNECTED 1")
endif()

include(${PROJECT_SOURCE_DIR}/cmake/DownloadProject.cmake)
download_project(PROJ                fmt
                 GIT_REPOSITORY      https://github.com/fmtlib/fmt.git
                 GIT_TAG             3.0.0
                 ${UPDATE_DISCONNECTED_IF_AVAILABLE})
set(FMT_SRC ${CMAKE_BINARY_DIR}/fmt-src/fmt/format.cc)
set(FMT_INCLUDE_DIR ${CMAKE_BINARY_DIR}/fmt-src)
########################

##### Get rapidJSON ######
if (CMAKE_VERSION VERSION_LESS 3.2)
    set(UPDATE_DISCONNECTED_IF_AVAILABLE "")
else()
    set(UPDATE_DISCONNECTED_IF_AVAILABLE "UPDATE_DISCONNECTED 1")
endif()

include(${PROJECT_SOURCE_DIR}/cmake/DownloadProject.cmake)
download_project(PROJ                rapidJSON
                 GIT_REPOSITORY      https://github.com/miloyip/rapidjson.git
                 GIT_TAG             v1.1.0
                 ${UPDATE_DISCONNECTED_IF_AVAILABLE})
set(RAPIDJSON_INCLUDE_DIR ${CMAKE_BINARY_DIR}/rapidJSON-src/include)
########################

add_subdirectory(src)

option(RUN_DOXYGEN "Run doxygen" TRUE)
if (RUN_DOXYGEN)
configure_file(Doxygen.conf dg.conf)
add_custom_target(docs COMMAND doxygen dg.conf)
endif()
