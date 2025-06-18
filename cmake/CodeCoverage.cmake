function(enable_coverage_for_target target_name)
    if(COV AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        message(STATUS "Coverage enabled for target: ${target_name}")
        target_compile_options(${target_name} PRIVATE --coverage)
        target_link_options(${target_name} PRIVATE --coverage)
    endif()
endfunction()


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
