add_executable(kafka-to-nexus kafka-to-nexus.cpp)
#target_compile_features(app PRIVATE cxx_std_17)

target_link_libraries(kafka-to-nexus PRIVATE filewriter_lib ${CONAN_LIBS})