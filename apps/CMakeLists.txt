add_executable(kafka-to-nexus kafka-to-nexus.cpp)

target_link_libraries(kafka-to-nexus PRIVATE filewriter_lib ${CONAN_LIBS})