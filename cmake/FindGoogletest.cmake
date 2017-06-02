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

    function(add_gtest_to_target tgt)
        add_dependencies(${tgt} gtest)
        target_link_libraries(${tgt} gtest gmock)
    endfunction()
endif(BUILD_TESTING)
