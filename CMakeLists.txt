cmake_minimum_required(VERSION 2.8.11)
project("kafka-to-nexus")

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_SOURCE_DIR}/cmake")

if(CMAKE_COMPILER_IS_GNUCXX AND COV)
  include(CodeCoverage)
  set(exclude_from_coverage "*_generated.h" "src/date/*" "*/.conan/data/*")
  setup_target_for_coverage(coverage tests coverage ${exclude_from_coverage})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_COVERAGE}")
endif()

if(NOT CONAN_DISABLE)
  include(${CMAKE_MODULE_PATH}/conan.cmake)
  conan_cmake_run(CONANFILE conan/conanfile.txt
          PROFILE default
          BASIC_SETUP NO_OUTPUT_DIRS
          BUILD_TYPE "None"
          BUILD outdated)
endif()

# Yes, there exists also FindGTest.
# Yes, we do want to use this one instead:
find_package(Googletest)

add_subdirectory(src)

option(RUN_DOXYGEN "Run doxygen" TRUE)
if (RUN_DOXYGEN)
configure_file(Doxygen.conf dg.conf)
add_custom_target(docs COMMAND doxygen "${PROJECT_BINARY_DIR}/dg.conf" WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
endif()
