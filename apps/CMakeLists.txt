add_executable(kafka-to-nexus kafka-to-nexus.cpp)
target_link_libraries(kafka-to-nexus PRIVATE filewriter_lib ${CONAN_LIBS})

add_executable(file_maker ${sources} file_maker.cpp)
target_link_libraries(file_maker PRIVATE filewriter_lib ${CONAN_LIBS})
