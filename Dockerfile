FROM ubuntu:18.04

ARG http_proxy
ARG https_proxy
ARG local_conan_server

# Replace the default profile and remotes with the ones from our Ubuntu build node
#ADD "https://raw.githubusercontent.com/ess-dmsc/docker-ubuntu18.04-build-node/master/files/default_profile" "/root/.conan/profiles/default"
COPY ./conan ../kafka_to_nexus_src/conan

# Install packages - We don't want to purge kafkacat and tzdata after building
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update -y \
    && apt-get --no-install-recommends -y install build-essential git python python-pip cmake python-setuptools autoconf libtool automake kafkacat tzdata \
    && apt-get -y autoremove  \
    && apt-get clean all \
    && rm -rf /var/lib/apt/lists/* \
    && pip install conan \
    && mkdir kafka_to_nexus \
    && conan config install http://github.com/ess-dmsc/conan-configuration.git \
#    && if [ ! -z "$local_conan_server" ]; then conan remote add --insert 0 ess-dmsc-local "$local_conan_server"; fi \
    && cd kafka_to_nexus \
    && conan install --build=outdated ../kafka_to_nexus_src/conan/conanfile.txt

COPY ./cmake ../kafka_to_nexus_src/cmake
COPY ./src ../kafka_to_nexus_src/src
COPY ./CMakeLists.txt ../kafka_to_nexus_src/CMakeLists.txt

RUN cd kafka_to_nexus \
    && cmake -DCONAN="MANUAL" --target="kafka-to-nexus" -DCMAKE_BUILD_TYPE=Release -DUSE_GRAYLOG_LOGGER=True -DRUN_DOXYGEN=False -DBUILD_TESTS=False ../kafka_to_nexus_src \
    && make -j8 kafka-to-nexus \
    && mkdir /output-files \
    && conan remove "*" -s -f \
    && apt purge -y build-essential git python python-pip cmake python-setuptools autoconf libtool automake \
    && rm -rf ../../kafka_to_nexus_src/* \
    && rm -rf /tmp/* /var/tmp/* /kafka_to_nexus/src /root/.conan/

COPY ./docker_launch.sh ../docker_launch.sh
CMD ["/docker_launch.sh"]
