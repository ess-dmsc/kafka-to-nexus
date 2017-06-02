# Download rapidJSON
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
