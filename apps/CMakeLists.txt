find_package(CLI11 REQUIRED)

add_executable(kafka-to-nexus kafka-to-nexus.cpp)
target_link_libraries(kafka-to-nexus 
    PRIVATE 
        filewriter_lib 
        ${COMMON_LIBS}
        CLI11::CLI11
)

add_executable(file-maker ${sources} file-maker.cpp)
target_link_libraries(file-maker
  PRIVATE 
    filewriter_lib 
    ${COMMON_LIBS}
    CLI11::CLI11
)

add_executable(template-maker ${sources} template-maker.cpp)
target_link_libraries(template-maker
  PRIVATE 
    filewriter_lib 
    ${COMMON_LIBS}
    CLI11::CLI11
)
