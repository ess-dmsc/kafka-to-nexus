FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive

ARG http_proxy

ARG https_proxy

ARG local_conan_server

ENV BUILD_PACKAGES "build-essential git python python-pip cmake python-setuptools autoconf libtool automake"

# Install packages - We don't want to purge kafkacat and tzdata after building
RUN apt-get update -y && \
    apt-get --no-install-recommends -y install $BUILD_PACKAGES kafkacat tzdata && \
    apt-get -y autoremove && \
    apt-get clean all && \
    rm -rf /var/lib/apt/lists/* && \
    pip install conan && \
    conan profile new default && \
    mkdir kafka_to_nexus

# Replace the default profile and remotes with the ones from our Ubuntu build node
ADD "https://raw.githubusercontent.com/ess-dmsc/docker-ubuntu18.04-build-node/master/files/registry.json" "/root/.conan/registry.json"
ADD "https://raw.githubusercontent.com/ess-dmsc/docker-ubuntu18.04-build-node/master/files/default_profile" "/root/.conan/profiles/default"

# Add local Conan server
RUN if [ ! -z "$local_conan_server" ]; then conan remote add --insert 0 ess-dmsc-local "$local_conan_server"; fi

COPY ./conan ../kafka_to_nexus_src/conan
RUN cd kafka_to_nexus && conan install --build=outdated ../kafka_to_nexus_src/conan/conanfile.txt
COPY ./src ../kafka_to_nexus_src/src
COPY ./cmake ../kafka_to_nexus_src/cmake
COPY ./Doxygen.conf ./CMakeLists.txt ../kafka_to_nexus_src/
COPY docker_launch.sh /

RUN cd kafka_to_nexus && \
    cmake -DCONAN="MANUAL" -DCMAKE_BUILD_TYPE=Release -DUSE_GRAYLOG_LOGGER=True ../kafka_to_nexus_src && \
    make -j8 && mkdir /output-files && conan remove "*" -s -f && apt purge -y $BUILD_PACKAGES && rm -rf ../../kafka_to_nexus_src/* && rm -rf /tmp/* /var/tmp/*

CMD ["./docker_launch.sh"]
