FROM ubuntu:18.04

# Install packages
ENV BUILD_PACKAGES "build-essential git python python-pip cmake python-setuptools kafkacat"
RUN apt-get -y update && apt-get install $BUILD_PACKAGES -y --no-install-recommends && \
rm -rf /var/lib/apt/lists/*

RUN pip install conan
# Force conan to create .conan directory and profile
RUN conan profile new default

# Replace the default profile and remotes with the ones from our Ubuntu build node
ADD "https://raw.githubusercontent.com/ess-dmsc/docker-ubuntu18.04-build-node/master/files/registry.txt" "/root/.conan/registry.txt"
ADD "https://raw.githubusercontent.com/ess-dmsc/docker-ubuntu18.04-build-node/master/files/default_profile" "/root/.conan/profiles/default"

RUN mkdir kafka_to_nexus
RUN cd kafka_to_nexus
COPY ./src ../kafka_to_nexus_src/src
COPY ./conan ../kafka_to_nexus_src/conan
COPY ./CMakeLists.txt ../kafka_to_nexus_src/CMakeLists.txt
COPY ./cmake ../kafka_to_nexus_src/cmake
COPY ./Doxygen.conf ../kafka_to_nexus_src/Doxygen.conf

RUN cd kafka_to_nexus && \
    cmake -DUSE_GRAYLOG_LOGGER=True ../kafka_to_nexus_src && \
    make -j8

RUN mkdir /output-files
COPY docker_launch.sh /

CMD ["./docker_launch.sh"]
