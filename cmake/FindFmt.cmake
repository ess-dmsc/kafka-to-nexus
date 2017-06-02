# Download fmt
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
